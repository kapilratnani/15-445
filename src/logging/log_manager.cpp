/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"

namespace cmudb {
  /*
   * set ENABLE_LOGGING = true
   * Start a separate thread to execute flush to disk operation periodically
   * The flush can be triggered when the log buffer is full or buffer pool
   * manager wants to force flush (it only happens when the flushed page has a
   * larger LSN than persistent LSN)
   */
  void LogManager::RunFlushThread() {
    std::lock_guard<std::mutex> g(latch_);
    if (!flush_thread_on) {
      ENABLE_LOGGING = true;
      flush_thread_on = true;
      flush_thread_ = new std::thread(&LogManager::BgFSync, this);
    }
  }

  void LogManager::BgFSync() {
    while (flush_thread_on)
    {
      {
        std::unique_lock<std::mutex> lock(latch_);
        // check for spurious wakeups
        while (log_buffer_size_ == 0) {
          cv_.wait_for(lock, LOG_TIMEOUT);
          if (!flush_thread_on)
            break;
        }
        SwapBuffers();
      }
      disk_manager_->WriteLog(flush_buffer_, flush_buffer_size);
      std::unique_lock<std::mutex> lock(latch_);
      auto lsn = LastLsn(flush_buffer_, flush_buffer_size);
      if (lsn != INVALID_LSN) {
        persistent_lsn_ = lsn;
      }
      flush_buffer_size = 0;
      flushed_.notify_all();
    }
  }

  void LogManager::SwapBuffers() {
    std::swap(flush_buffer_, log_buffer_);
    flush_buffer_size = log_buffer_size_;
    log_buffer_size_ = 0;
  }

  lsn_t LogManager::LastLsn(char* buf, int size) {
    lsn_t lsn = INVALID_LSN;
    char* ptr = buf;
    while (ptr < buf + size) {
      auto* record = reinterpret_cast<LogRecord*>(ptr);
      lsn = record->GetLSN();
      ptr += record->GetSize();
    }
    return lsn;
  }

  /*
   * Stop and join the flush thread, set ENABLE_LOGGING = false
   */
  void LogManager::StopFlushThread() {
    std::unique_lock<std::mutex> g(latch_);
    if (flush_thread_on) {
      ENABLE_LOGGING = false;
      flush_thread_on = false;
      {
        // unlock, so that bgfsync can proceed
        g.unlock();
        cv_.notify_all();
      }
      flush_thread_->join();
      g.lock();
      delete flush_thread_;
    }
  }

  void LogManager::WakeUpFlushThread() {
    cv_.notify_all();
  }

  void LogManager::WaitTillFlushHappens() {
    std::unique_lock<std::mutex> g(latch_);
    do{
      flushed_.wait(g);
    } while (flush_buffer_size != 0);
  }

  /*
   * append a log record into log buffer
   * you MUST set the log record's lsn within this method
   * @return: lsn that is assigned to this log record
   *
   *
   * example below
   * // First, serialize the must have fields(20 bytes in total)
   * log_record.lsn_ = next_lsn_++;
   * memcpy(log_buffer_ + offset_, &log_record, 20);
   * int pos = offset_ + 20;
   *
   * if (log_record.log_record_type_ == LogRecordType::INSERT) {
   *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
   *    pos += sizeof(RID);
   *    // we have provided serialize function for tuple class
   *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
   *  }
   *
   */
  lsn_t LogManager::AppendLogRecord(LogRecord& log_record) {

    // check if buffer is full
    int size = log_record.GetSize();
    if (size + log_buffer_size_ > LOG_BUFFER_SIZE) {
      WakeUpFlushThread();
      WaitTillFlushHappens();
    }

    /*
      1. assign lsn
      2. copy header to log_buffer
      3. based on type of log_record copy required fields
    */
    log_record.lsn_ = next_lsn_++;
    int offset = log_buffer_size_;
    memcpy(log_buffer_ + offset, &log_record, 20);
    offset += 20;
    if (log_record.log_record_type_ == LogRecordType::INSERT) {
      memcpy(log_buffer_ + offset, &log_record.insert_rid_, sizeof(RID));
      offset += sizeof(RID);
      log_record.insert_tuple_.SerializeTo(log_buffer_ + offset);
    }
    else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE
      || log_record.log_record_type_ == LogRecordType::MARKDELETE
      || log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
      memcpy(log_buffer_ + offset, &log_record.delete_rid_, sizeof(RID));
      offset += sizeof(RID);
      log_record.delete_tuple_.SerializeTo(log_buffer_ + offset);
    }
    else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
      memcpy(log_buffer_ + offset, &log_record.update_rid_, sizeof(RID));
      offset += sizeof(RID);
      log_record.old_tuple_.SerializeTo(log_buffer_ + offset);
      offset += log_record.old_tuple_.GetLength() + sizeof(int32_t);
      log_record.new_tuple_.SerializeTo(log_buffer_ + offset);
    }
    else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
      memcpy(log_buffer_ + offset, &log_record.prev_page_id_, sizeof(page_id_t));
    }
    log_buffer_size_ += log_record.GetSize();
    return log_record.lsn_;
  }

} // namespace cmudb
