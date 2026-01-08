/**
 * Copyright 2023 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

 #include <string.h>
 #include <stdlib.h>
 #include <assert.h>
 #include <errno.h>
 #include "db_client.h"
 #include "em_base.h"

 #ifndef DB_LOG
 #define DB_LOG(fmt, ...) printf("DB:%s:%d: " fmt "\n", __func__, __LINE__, ##__VA_ARGS__)
 #endif

 static const char *db_query_preview(const char *q, char *buf, size_t buf_len)
 {
     if (!buf || buf_len == 0) {
         return "";
     }
     if (!q) {
         buf[0] = '\0';
         return buf;
     }
     // Log only a short prefix to avoid huge lines.
     snprintf(buf, buf_len, "%.*s", static_cast<int>(buf_len - 1), q);
     return buf;
 }

 // Structure to hold the result set and associated data
 struct result_context_t {
     MYSQL_RES *result;
     MYSQL_ROW row;
 };

 int db_client_t::recreate_db()
 {
     if (!m_con) {
         printf("%s:%d: No database connection\n", __func__, __LINE__);
         return -1;
     }

     // Drop existing database
     if (mysql_query(m_con, "DROP DATABASE IF EXISTS OneWifiMesh")) {
         printf("%s:%d: Error dropping database: %s\n", __func__, __LINE__, mysql_error(m_con));
         return -1;
     }

     // Create new database
     if (mysql_query(m_con, "CREATE DATABASE OneWifiMesh")) {
         printf("%s:%d: Error creating database: %s\n", __func__, __LINE__, mysql_error(m_con));
         return -1;
     }

     return 0;
 }

 void *db_client_t::execute(const char *query)
 {
     if (!m_con) {
         DB_LOG("m_con is NULL, cannot execute query");
         return NULL;
     }

     if (!query) {
         DB_LOG("query is NULL");
         return NULL;
     }

     // Best-effort liveness check (helps identify server drop vs client crash).
     if (mysql_ping(m_con) != 0) {
         DB_LOG("mysql_ping failed: errno=%u err=%s", mysql_errno(m_con), mysql_error(m_con));
     }

     auto run_query_once = [&](void) -> bool {
         if (mysql_query(m_con, query) != 0) {
             char preview[160];
             DB_LOG("mysql_query failed: errno=%u err=%s query_len=%zu query_prefix='%s'",
                    mysql_errno(m_con), mysql_error(m_con), strlen(query),
                    db_query_preview(query, preview, sizeof(preview)));
             return false;
         }
         return true;
     };

     if (!run_query_once()) {
         // Retry once if connection was lost (helps avoid transient disconnects).
         const unsigned int err = mysql_errno(m_con);
         if (err == 2006 /* CR_SERVER_GONE_ERROR */ || err == 2013 /* CR_SERVER_LOST */) {
             DB_LOG("retrying once after server-lost error (%u)", err);
             (void)mysql_ping(m_con);
             if (!run_query_once()) {
                 return NULL;
             }
         } else {
             return NULL;
         }
     }

     MYSQL_RES *result = mysql_store_result(m_con);
     if (!result) {
         // This might not be an error - could be a query that doesn't return results (INSERT, UPDATE, etc.)
         if (mysql_field_count(m_con) == 0) {
             return NULL;  // Query was successful but didn't return data
         } else {
             DB_LOG("mysql_store_result failed: errno=%u err=%s", mysql_errno(m_con), mysql_error(m_con));
             return NULL;
         }
     }

     // Create a context structure to hold the result and current row
     result_context_t *ctx = new result_context_t;
     ctx->result = result;
     ctx->row = NULL;

     return ctx;
 }

 bool db_client_t::next_result(void *ctx)
 {
     if (ctx == NULL) {
         return false;
     }

     result_context_t *res_ctx = static_cast<result_context_t *>(ctx);
     res_ctx->row = mysql_fetch_row(res_ctx->result);

     if (res_ctx->row == NULL) {
         // No more rows - clean up
         mysql_free_result(res_ctx->result);
         delete res_ctx;
         return false;
     }

     return true;
 }

 char *db_client_t::get_string(void *ctx, char *str, size_t str_len, unsigned int col)
 {
     if (ctx == NULL || str == NULL || str_len == 0) {
         return NULL;
     }

     result_context_t *res_ctx = static_cast<result_context_t *>(ctx);

     if (col == 0) {
         return NULL;
     }

     const unsigned int num_fields = mysql_num_fields(res_ctx->result);
     if (col > num_fields) {
         return NULL;
     }

     if (res_ctx->row == NULL || res_ctx->row[col - 1] == NULL) {
         return NULL;
     }

     // Note: Column indices in MariaDB C API are 0-based
     unsigned long *lengths = mysql_fetch_lengths(res_ctx->result);
     if (!lengths) {
         return NULL;
     }

     const unsigned long src_len = lengths[col - 1];
     const size_t copy_len = (src_len < (str_len - 1)) ? static_cast<size_t>(src_len) : (str_len - 1);
     memcpy(str, res_ctx->row[col - 1], copy_len);
     str[copy_len] = '\0';
     return str;
 }

 int db_client_t::get_number(void *ctx, unsigned int col)
 {
     assert(ctx != NULL);

     result_context_t *res_ctx = static_cast<result_context_t *>(ctx);

     if (res_ctx->row == NULL || res_ctx->row[col - 1] == NULL) {
         return 0;
     }

     // Note: Column indices in MariaDB C API are 0-based
     return atoi(res_ctx->row[col - 1]);
 }

 int db_client_t::connect(const char *path)
 {
     if (path == NULL || strlen(path) <= 0) {
         return -1;
     }

     // Parse the path format: "username@password"
     char *tmp = strchr(const_cast<char *>(path), '@');
     if (tmp == NULL) {
         printf("%s:%d: invalid path: %s\n", __func__, __LINE__, path);
         return -1;
     }

     // Split username and password
     char username[256];
     char password[256];

     size_t user_len = static_cast<size_t>(tmp - path);
     if (user_len >= sizeof(username)) {
         printf("%s:%d: username too long\n", __func__, __LINE__);
         return -1;
     }

     strncpy(username, path, user_len);
     username[user_len] = '\0';

     tmp++; // Move past '@'
     strncpy(password, tmp, sizeof(password) - 1);
     password[sizeof(password) - 1] = '\0';

     DB_LOG("connecting to MySQL: user='%s' host='localhost' port=%d db='%s'", username, 3306, "OneWifiMesh");

     // Initialize MySQL connection
     m_con = mysql_init(NULL);
     if (m_con == NULL) {
         DB_LOG("mysql_init() failed");
         return -1;
     }

     // Connection options to reduce ambiguous disconnects and help debugging.
     {
         unsigned int timeout = 5;
         (void)mysql_options(m_con, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
         (void)mysql_options(m_con, MYSQL_OPT_READ_TIMEOUT, &timeout);
         (void)mysql_options(m_con, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
#ifdef MYSQL_OPT_RECONNECT
         my_bool reconnect = 1;
         (void)mysql_options(m_con, MYSQL_OPT_RECONNECT, &reconnect);
#endif
     }

     // Connect to the database
     if (mysql_real_connect(m_con,
                           "localhost",
                           username,
                           password,
                           NULL,        // Don't select database yet
                           3306,       // Default port
                           NULL,       // Unix socket
                           0) == NULL) {
         DB_LOG("mysql_real_connect() failed: errno=%u err=%s", mysql_errno(m_con), mysql_error(m_con));
         mysql_close(m_con);
         m_con = NULL;
         return -1;
     }

     // Select the database
     if (mysql_select_db(m_con, "OneWifiMesh") != 0) {
         DB_LOG("mysql_select_db('OneWifiMesh') failed: errno=%u err=%s (db may not exist yet)",
                mysql_errno(m_con), mysql_error(m_con));
         // Don't fail here - the database might not exist yet
     }

     return 0;
 }

 int db_client_t::init(const char *path)
 {
     if (connect(path) != 0) {
         printf("%s:%d: Connect failed\n", __func__, __LINE__);
         return -1;
     }

     return 0;
 }

 db_client_t::db_client_t()
 {
     m_con = NULL;
 }

 db_client_t::~db_client_t()
 {
     if (m_con) {
         mysql_close(m_con);
         m_con = NULL;
     }
 }
