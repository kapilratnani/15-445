/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
  /*
   * deserialize a log record from log buffer
   * @return: true means deserialize succeed, otherwise can't deserialize cause
   * incomplete log record
   */
  bool LogRecovery::DeserializeLogRecord(const char* data,
    LogRecord& log_record) {
    LogRecord* ptr = reinterpret_cast<LogRecord*>(const_cast<char*>(data));
    log_record.size_ = ptr->GetSize();

    if (log_record.size_ == 0)
      return false;
    log_record.log_record_type_ = ptr->GetLogRecordType();
    if (log_record.log_record_type_ == LogRecordType::INVALID)
      return false;

    log_record.lsn_ = ptr->GetLSN();
    log_record.txn_id_ = ptr->GetTxnId();
    log_record.prev_lsn_ = ptr->GetPrevLSN();
    char* pos = const_cast<char*>(data) + LogRecord::HEADER_SIZE;
    switch (log_record.log_record_type_) {
    case LogRecordType::INSERT:
      log_record.insert_rid_ = *reinterpret_cast<RID*>(pos);
      log_record.insert_tuple_.DeserializeFrom(pos + sizeof(RID));
      break;
    case LogRecordType::APPLYDELETE:
      log_record.delete_rid_ = *reinterpret_cast<RID*>(pos);
      log_record.delete_tuple_.DeserializeFrom(pos + sizeof(RID));
      break;
    case LogRecordType::ROLLBACKDELETE:
    case LogRecordType::MARKDELETE:
      log_record.delete_rid_ = *reinterpret_cast<RID*>(pos);
      break;
    case LogRecordType::UPDATE:
      log_record.update_rid_ = *reinterpret_cast<RID*>(pos);
      log_record.old_tuple_.DeserializeFrom(pos + sizeof(RID));
      log_record.new_tuple_.DeserializeFrom(pos + sizeof(RID) + log_record.old_tuple_.GetLength());
      break;
    case LogRecordType::NEWPAGE:
      log_record.prev_page_id_ = *reinterpret_cast<page_id_t*>(pos);
    default:
      break;
    }
    return true;
  }

  /*
   *redo phase on TABLE PAGE level(table/table_page.h)
   *read log file from the beginning to end (you must prefetch log records into
   *log buffer to reduce unnecessary I/O operations), remember to compare page's
   *LSN with log_record's sequence number, and also build active_txn_ table &
   *lsn_mapping_ table
   */
  void LogRecovery::Redo() {
    ENABLE_LOGGING = false;
    offset_ = 0;
    int limit = LOG_BUFFER_SIZE;
    bool hasLogs = disk_manager_->ReadLog(log_buffer_, limit, offset_);
    while (hasLogs) {
      auto* log = log_buffer_;
      LogRecord logRecord;
      int log_size_read = 0;
      TablePage* table_page = nullptr;
      while (DeserializeLogRecord(log, logRecord)) {
        auto rid = logRecord.insert_rid_;
        switch (logRecord.log_record_type_)
        {
        case LogRecordType::BEGIN:
          active_txn_[logRecord.txn_id_] = logRecord.GetLSN();
          break;
        case LogRecordType::ABORT:
        case LogRecordType::COMMIT:
          active_txn_.erase(logRecord.txn_id_);
          break;
        case LogRecordType::INSERT:
          active_txn_[logRecord.txn_id_] = logRecord.GetLSN();
          table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
          if (table_page->GetLSN() >= logRecord.GetLSN())
            continue;
          table_page->InsertTuple(logRecord.insert_tuple_, rid, nullptr, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::UPDATE:
          active_txn_[logRecord.txn_id_] = logRecord.GetLSN();
          table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
          if (table_page->GetLSN() >= logRecord.GetLSN())
            continue;
          table_page->UpdateTuple(logRecord.new_tuple_, logRecord.old_tuple_, rid, nullptr, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::MARKDELETE:
          active_txn_[logRecord.txn_id_] = logRecord.GetLSN();
          table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
          if (table_page->GetLSN() >= logRecord.GetLSN())
            continue;
          table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::ROLLBACKDELETE:
          active_txn_[logRecord.txn_id_] = logRecord.GetLSN();
          table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
          if (table_page->GetLSN() >= logRecord.GetLSN())
            continue;
          table_page->RollbackDelete(rid, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::APPLYDELETE:
          active_txn_[logRecord.txn_id_] = logRecord.GetLSN();
          table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
          if (table_page->GetLSN() >= logRecord.GetLSN())
            continue;
          table_page->ApplyDelete(rid, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::NEWPAGE:
          active_txn_[logRecord.txn_id_] = logRecord.GetLSN();
          table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(logRecord.GetNewPageRecord()));
          table_page->Init(logRecord.GetNewPageRecord(), PAGE_SIZE, INVALID_PAGE_ID, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(logRecord.GetNewPageRecord(), true);
          break;
        default:
          break;
        }

        lsn_mapping_[logRecord.GetLSN()] = offset_ + log_size_read;
        log_size_read += logRecord.GetSize();
        log += logRecord.GetSize();
      }

      offset_ = limit + LOG_BUFFER_SIZE;
      hasLogs = disk_manager_->ReadLog(log_buffer_, limit, offset_);
    }
    ENABLE_LOGGING = true;
  }

  /*
   *undo phase on TABLE PAGE level(table/table_page.h)
   *iterate through active txn map and undo each operation
   */
  void LogRecovery::Undo() {
    ENABLE_LOGGING = false;
    for (auto item : active_txn_) {
      auto lsn = item.second;
      while (true)
      {
        auto offset = lsn_mapping_[lsn];
        bool hasLogs = disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset);
        if (!hasLogs)
          break;

        LogRecord logRecord;
        if (!DeserializeLogRecord(log_buffer_, logRecord))
          break;
        auto rid = logRecord.insert_rid_;
        TablePage* table_page = nullptr;
        switch (logRecord.log_record_type_) {
          case LogRecordType::INSERT:
            table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
            if (table_page->GetLSN() >= logRecord.GetLSN())
              continue;
            table_page->ApplyDelete(rid, nullptr, nullptr);
            buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
            break;
          case LogRecordType::MARKDELETE:
            table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
            if (table_page->GetLSN() >= logRecord.GetLSN())
              continue;
            table_page->RollbackDelete(rid, nullptr, nullptr);
            buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
            break;
          case LogRecordType::UPDATE:
            table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
            if (table_page->GetLSN() >= logRecord.GetLSN())
              continue;
            table_page->UpdateTuple(logRecord.old_tuple_, logRecord.new_tuple_, rid, nullptr, nullptr, nullptr);
            buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
            break;
          default:
            break;
        }
        lsn = logRecord.GetPrevLSN();
        if (lsn == INVALID_LSN)
          break;
      }
    }
  }

} // namespace cmudb
