/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "lib/thread/thread_pool.h"
#include <atomic>
#include <mutex>
#include <string>

namespace oceanbase { namespace observer {
class AiColumnService : public lib::ThreadPool
{
public:
  static AiColumnService &instance();
  int start_service();
  void stop_service();
  int install_table(uint64_t table_id, bool initialize_rows);
  void run1() override;
private:
  AiColumnService() = default;
  int recover();
  int process_group();
  std::mutex lifecycle_mutex_;
  std::mutex install_mutex_;
  bool started_ = false;
  std::atomic<bool> recovered_{false};
  std::atomic<bool> has_ai_columns_{false};
};
}}
