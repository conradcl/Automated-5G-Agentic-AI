/*
 * Licensed to the OpenAirInterface (OAI) Software Alliance under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The OpenAirInterface Software Alliance licenses this file to You under
 * the OAI Public License, Version 1.1  (the "License"); you may not use this file
 * except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.openairinterface.org/?page_id=698
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BAS
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *-------------------------------------------------------------------------------
 * For more information about the OpenAirInterface (OAI) Software Alliance:
 *      contact@openairinterface.org
 */

#include "../../../../src/xApp/e42_xapp_api.h"
#include "../../../../src/util/alg_ds/alg/defer.h"
#include "../../../../src/util/time_now_us.h"
#include "../../../../src/util/alg_ds/ds/lock_guard/lock_guard.h"
#include "../../../../src/util/e.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <ctype.h>
#include <curl/curl.h>
#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>

static uint64_t const period_ms = 1000;

static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
static volatile sig_atomic_t stop_requested = 0;

#define DEFAULT_EVIDENCE_API_URL "http://127.0.0.1:9991/v1/evidence"
#define DEFAULT_PUBLISH_INTERVAL_MS 1000
#define DEFAULT_HTTP_CONNECT_TIMEOUT_MS 1000
#define DEFAULT_HTTP_TIMEOUT_MS 10000
#define JSON_BUFFER_SIZE 8192
#define SOURCE_INSTANCE_ID_SIZE 128

typedef struct
{
  double value;
  int available;
} metric_value_t;

typedef struct
{
  int ric_connected;
  int e2_nodes_connected;
  unsigned long long kpm_indications_received;
  long long last_kpm_received_at_us;
  long long last_kpm_collect_start_time;
  long long last_kpm_latency_us;
  int latency_available;
  int incomplete;
  metric_value_t prb_tot_dl;
  metric_value_t prb_tot_ul;
  metric_value_t pdcp_volume_dl;
  metric_value_t pdcp_volume_ul;
  metric_value_t rlc_delay_dl;
  metric_value_t ue_throughput_dl;
  metric_value_t ue_throughput_ul;
} observation_state_t;

static observation_state_t observation = {0};
static char evidence_api_url[512] = DEFAULT_EVIDENCE_API_URL;
static char source_instance_id[SOURCE_INSTANCE_ID_SIZE] = {0};
static unsigned long publish_interval_ms = DEFAULT_PUBLISH_INTERVAL_MS;
static unsigned long http_connect_timeout_ms = DEFAULT_HTTP_CONNECT_TIMEOUT_MS;
static unsigned long http_timeout_ms = DEFAULT_HTTP_TIMEOUT_MS;
static unsigned long long publish_sequence = 0;

typedef struct
{
  char *buffer;
  size_t capacity;
  size_t length;
  int failed;
} json_builder_t;

static void request_stop(int signal_number)
{
  (void)signal_number;
  stop_requested = 1;
}

static void install_signal_handlers(void)
{
  struct sigaction action = {0};
  action.sa_handler = request_stop;
  sigemptyset(&action.sa_mask);
  sigaction(SIGINT, &action, NULL);
  sigaction(SIGTERM, &action, NULL);
}

static long long realtime_now_us(void)
{
  struct timespec now = {0};
  clock_gettime(CLOCK_REALTIME, &now);
  return (long long)now.tv_sec * 1000000LL + now.tv_nsec / 1000LL;
}

static int format_epoch_us(long long epoch_us, char *destination, size_t size)
{
  if (epoch_us <= 0 || destination == NULL || size == 0)
    return -1;

  time_t seconds = (time_t)(epoch_us / 1000000LL);
  long long microseconds = epoch_us % 1000000LL;
  struct tm utc = {0};
  if (gmtime_r(&seconds, &utc) == NULL)
    return -1;

  int written = snprintf(destination,
                         size,
                         "%04d-%02d-%02dT%02d:%02d:%02d.%06lldZ",
                         utc.tm_year + 1900,
                         utc.tm_mon + 1,
                         utc.tm_mday,
                         utc.tm_hour,
                         utc.tm_min,
                         utc.tm_sec,
                         microseconds);
  return written > 0 && (size_t)written < size ? 0 : -1;
}

static unsigned long read_positive_env(const char *name, unsigned long fallback)
{
  const char *raw = getenv(name);
  if (raw == NULL || *raw == '\0')
    return fallback;

  char *end = NULL;
  errno = 0;
  unsigned long value = strtoul(raw, &end, 10);
  if (errno == ERANGE || end == raw || *end != '\0' || value == 0 || value > 600000)
  {
    fprintf(stderr, "Ignoring invalid %s=%s\n", name, raw);
    return fallback;
  }
  return value;
}

static void sanitize_identifier(char *value)
{
  for (size_t i = 0; value[i] != '\0'; ++i)
  {
    unsigned char current = (unsigned char)value[i];
    if (!isalnum(current) && current != '-' && current != '_' && current != '.')
      value[i] = '-';
  }
}

static void configure_publisher(void)
{
  const char *configured_url = getenv("EVIDENCE_API_URL");
  if (configured_url != NULL && *configured_url != '\0')
    snprintf(evidence_api_url, sizeof(evidence_api_url), "%s", configured_url);

  struct timespec started = {0};
  clock_gettime(CLOCK_REALTIME, &started);
  long long boot_id = (long long)started.tv_sec * 1000000000LL + started.tv_nsec;

  const char *configured_instance = getenv("HEALTH_XAPP_INSTANCE_ID");
  if (configured_instance != NULL && *configured_instance != '\0')
  {
    snprintf(source_instance_id,
             sizeof(source_instance_id),
             "%.80s-%lld",
             configured_instance,
             boot_id);
  }
  else
  {
    char hostname[64] = "unknown-host";
    if (gethostname(hostname, sizeof(hostname) - 1) != 0)
      snprintf(hostname, sizeof(hostname), "unknown-host");
    snprintf(source_instance_id,
             sizeof(source_instance_id),
             "%s-%ld-%lld",
             hostname,
             (long)getpid(),
             boot_id);
  }
  sanitize_identifier(source_instance_id);

  publish_interval_ms = read_positive_env(
      "EVIDENCE_PUBLISH_INTERVAL_MS", DEFAULT_PUBLISH_INTERVAL_MS);
  http_connect_timeout_ms = read_positive_env(
      "EVIDENCE_HTTP_CONNECT_TIMEOUT_MS", DEFAULT_HTTP_CONNECT_TIMEOUT_MS);
  http_timeout_ms = read_positive_env(
      "EVIDENCE_HTTP_TIMEOUT_MS", DEFAULT_HTTP_TIMEOUT_MS);
}

static void json_append(json_builder_t *builder, const char *format, ...)
{
  if (builder == NULL || builder->failed || builder->length >= builder->capacity)
    return;

  va_list arguments;
  va_start(arguments, format);
  int written = vsnprintf(builder->buffer + builder->length,
                          builder->capacity - builder->length,
                          format,
                          arguments);
  va_end(arguments);

  if (written < 0 || (size_t)written >= builder->capacity - builder->length)
  {
    builder->failed = 1;
    return;
  }
  builder->length += (size_t)written;
}

static void append_metric(json_builder_t *builder,
                          int *first,
                          const char *name,
                          metric_value_t metric,
                          const char *unit)
{
  if (!metric.available)
    return;
  json_append(builder,
              "%s\"%s\":{\"value\":%.17g,\"unit\":\"%s\",\"valid\":true}",
              *first ? "" : ",",
              name,
              metric.value,
              unit);
  *first = 0;
}

static void append_missing_metric(json_builder_t *builder,
                                  int *first,
                                  const char *name,
                                  metric_value_t metric)
{
  if (metric.available)
    return;
  json_append(builder, "%s\"%s\"", *first ? "" : ",", name);
  *first = 0;
}

static int build_evidence_json(const observation_state_t *snapshot,
                               char *destination,
                               size_t size)
{
  char observed_at[40] = {0};
  char last_kpm_at[40] = {0};
  if (format_epoch_us(realtime_now_us(), observed_at, sizeof(observed_at)) != 0)
    return -1;
  int has_last_kpm = snapshot->last_kpm_received_at_us > 0 &&
                     format_epoch_us(snapshot->last_kpm_received_at_us,
                                     last_kpm_at,
                                     sizeof(last_kpm_at)) == 0;

  json_builder_t builder = {
      .buffer = destination,
      .capacity = size,
      .length = 0,
      .failed = 0,
  };
  json_append(&builder,
              "{\"schema_version\":\"1.0\","
              "\"source\":\"health-xapp\","
              "\"source_instance_id\":\"%s\","
              "\"sequence_number\":%llu,"
              "\"observed_at\":\"%s\","
              "\"ric_connected\":%s,"
              "\"e2_nodes_connected\":%d,"
              "\"kpm_indications_received\":%llu,"
              "\"last_kpm_indication_at\":",
              source_instance_id,
              ++publish_sequence,
              observed_at,
              snapshot->ric_connected ? "true" : "false",
              snapshot->e2_nodes_connected,
              snapshot->kpm_indications_received);
  if (has_last_kpm)
    json_append(&builder, "\"%s\"", last_kpm_at);
  else
    json_append(&builder, "null");

  json_append(&builder, ",\"metrics\":{");
  int first = 1;
  append_metric(&builder, &first, "RRU.PrbTotDl", snapshot->prb_tot_dl, "PRB");
  append_metric(&builder, &first, "RRU.PrbTotUl", snapshot->prb_tot_ul, "PRB");
  append_metric(&builder, &first, "DRB.PdcpSduVolumeDL", snapshot->pdcp_volume_dl, "kb");
  append_metric(&builder, &first, "DRB.PdcpSduVolumeUL", snapshot->pdcp_volume_ul, "kb");
  append_metric(&builder, &first, "DRB.RlcSduDelayDl", snapshot->rlc_delay_dl, "us");
  append_metric(&builder, &first, "DRB.UEThpDl", snapshot->ue_throughput_dl, "kbps");
  append_metric(&builder, &first, "DRB.UEThpUl", snapshot->ue_throughput_ul, "kbps");
  if (snapshot->latency_available)
  {
    metric_value_t latency = {
        .value = (double)snapshot->last_kpm_latency_us,
        .available = 1,
    };
    append_metric(&builder, &first, "KPM.IndicationLatency", latency, "us");
  }

  json_append(&builder, "},\"missing_metrics\":[");
  first = 1;
  append_missing_metric(&builder, &first, "RRU.PrbTotDl", snapshot->prb_tot_dl);
  append_missing_metric(&builder, &first, "RRU.PrbTotUl", snapshot->prb_tot_ul);
  append_missing_metric(&builder, &first, "DRB.RlcSduDelayDl", snapshot->rlc_delay_dl);
  append_missing_metric(&builder, &first, "DRB.UEThpDl", snapshot->ue_throughput_dl);
  append_missing_metric(&builder, &first, "DRB.UEThpUl", snapshot->ue_throughput_ul);
  json_append(&builder,
              "],\"incomplete\":%s}",
              snapshot->incomplete ? "true" : "false");

  return builder.failed ? -1 : (int)builder.length;
}

static size_t discard_http_response(void *data, size_t size, size_t count, void *user_data)
{
  (void)data;
  (void)user_data;
  return size * count;
}

static void wait_for_next_publication(void)
{
  unsigned long waited_ms = 0;
  while (!stop_requested && waited_ms < publish_interval_ms)
  {
    unsigned long slice_ms = publish_interval_ms - waited_ms;
    if (slice_ms > 100)
      slice_ms = 100;
    usleep(slice_ms * 1000);
    waited_ms += slice_ms;
  }
}

static void *evidence_publisher_thread(void *unused)
{
  (void)unused;
  CURL *curl = curl_easy_init();
  if (curl == NULL)
  {
    fprintf(stderr, "Could not initialize the Evidence API HTTP client\n");
    stop_requested = 1;
    return NULL;
  }

  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  if (headers == NULL)
  {
    fprintf(stderr, "Could not allocate Evidence API HTTP headers\n");
    curl_easy_cleanup(curl);
    stop_requested = 1;
    return NULL;
  }
  curl_easy_setopt(curl, CURLOPT_URL, evidence_api_url);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long)http_connect_timeout_ms);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)http_timeout_ms);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_http_response);
#if LIBCURL_VERSION_NUM >= 0x075500
  curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
#else
  curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
#endif

  int first_success_logged = 0;
  while (!stop_requested)
  {
    observation_state_t snapshot = {0};
    {
      lock_guard(&mtx);
      snapshot = observation;
    }

    char payload[JSON_BUFFER_SIZE] = {0};
    int payload_length = build_evidence_json(&snapshot, payload, sizeof(payload));
    if (payload_length < 0)
    {
      fprintf(stderr, "Could not serialize Health xApp evidence\n");
      wait_for_next_publication();
      continue;
    }

    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)payload_length);
    CURLcode result = curl_easy_perform(curl);
    long status_code = 0;
    if (result == CURLE_OK)
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);

    if (result != CURLE_OK || status_code < 200 || status_code >= 300)
    {
      fprintf(stderr,
              "Evidence publish failed: curl=%s http_status=%ld target=%s\n",
              curl_easy_strerror(result),
              status_code,
              evidence_api_url);
    }
    else if (!first_success_logged)
    {
      printf("Publishing Health xApp evidence to %s\n", evidence_api_url);
      first_success_logged = 1;
    }
    wait_for_next_publication();
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  return NULL;
}

static void log_gnb_ue_id(ue_id_e2sm_t ue_id)
{
  if (ue_id.gnb.gnb_cu_ue_f1ap_lst != NULL)
  {
    for (size_t i = 0; i < ue_id.gnb.gnb_cu_ue_f1ap_lst_len; i++)
    {
      printf("UE ID type = gNB-CU, gnb_cu_ue_f1ap = %u\n", ue_id.gnb.gnb_cu_ue_f1ap_lst[i]);
    }
  }
  else
  {
    printf("UE ID type = gNB, amf_ue_ngap_id = %lu\n", ue_id.gnb.amf_ue_ngap_id);
  }
  if (ue_id.gnb.ran_ue_id != NULL)
  {
    printf("ran_ue_id = %lx\n", *ue_id.gnb.ran_ue_id); // RAN UE NGAP ID
  }
}

static void log_du_ue_id(ue_id_e2sm_t ue_id)
{
  printf("UE ID type = gNB-DU, gnb_cu_ue_f1ap = %u\n", ue_id.gnb_du.gnb_cu_ue_f1ap);
  if (ue_id.gnb_du.ran_ue_id != NULL)
  {
    printf("ran_ue_id = %lx\n", *ue_id.gnb_du.ran_ue_id); // RAN UE NGAP ID
  }
}

static void log_cuup_ue_id(ue_id_e2sm_t ue_id)
{
  printf("UE ID type = gNB-CU-UP, gnb_cu_cp_ue_e1ap = %u\n", ue_id.gnb_cu_up.gnb_cu_cp_ue_e1ap);
  if (ue_id.gnb_cu_up.ran_ue_id != NULL)
  {
    printf("ran_ue_id = %lx\n", *ue_id.gnb_cu_up.ran_ue_id); // RAN UE NGAP ID
  }
}

typedef void (*log_ue_id)(ue_id_e2sm_t ue_id);

static log_ue_id log_ue_id_e2sm[END_UE_ID_E2SM] = {
    log_gnb_ue_id, // common for gNB-mono, CU and CU-CP
    log_du_ue_id,
    log_cuup_ue_id,
    NULL,
    NULL,
    NULL,
    NULL,
};

static void log_int_value(byte_array_t name, meas_record_lst_t meas_record)
{
  if (cmp_str_ba("RRU.PrbTotDl", name) == 0)
  {
    printf("RRU.PrbTotDl = %lld [PRBs]\n", (long long)meas_record.int_val);
    observation.prb_tot_dl.value = meas_record.int_val;
    observation.prb_tot_dl.available = 1;
  }
  else if (cmp_str_ba("RRU.PrbTotUl", name) == 0)
  {
    printf("RRU.PrbTotUl = %lld [PRBs]\n", (long long)meas_record.int_val);
    observation.prb_tot_ul.value = meas_record.int_val;
    observation.prb_tot_ul.available = 1;
  }
  else if (cmp_str_ba("DRB.PdcpSduVolumeDL", name) == 0)
  {
    printf("DRB.PdcpSduVolumeDL = %lld [kb]\n", (long long)meas_record.int_val);
    observation.pdcp_volume_dl.value = meas_record.int_val;
    observation.pdcp_volume_dl.available = 1;
  }
  else if (cmp_str_ba("DRB.PdcpSduVolumeUL", name) == 0)
  {
    printf("DRB.PdcpSduVolumeUL = %lld [kb]\n", (long long)meas_record.int_val);
    observation.pdcp_volume_ul.value = meas_record.int_val;
    observation.pdcp_volume_ul.available = 1;
  }
  else
  {
    printf("Measurement Name not yet supported\n");
  }
}

static void log_real_value(byte_array_t name, meas_record_lst_t meas_record)
{
  if (!isfinite(meas_record.real_val))
  {
    observation.incomplete = 1;
    return;
  }
  if (cmp_str_ba("DRB.RlcSduDelayDl", name) == 0)
  {
    printf("DRB.RlcSduDelayDl = %.2f [us]\n", meas_record.real_val);
    observation.rlc_delay_dl.value = meas_record.real_val;
    observation.rlc_delay_dl.available = 1;
  }
  else if (cmp_str_ba("DRB.UEThpDl", name) == 0)
  {
    printf("DRB.UEThpDl = %.2f [kbps]\n", meas_record.real_val);
    observation.ue_throughput_dl.value = meas_record.real_val;
    observation.ue_throughput_dl.available = 1;
  }
  else if (cmp_str_ba("DRB.UEThpUl", name) == 0)
  {
    printf("DRB.UEThpUl = %.2f [kbps]\n", meas_record.real_val);
    observation.ue_throughput_ul.value = meas_record.real_val;
    observation.ue_throughput_ul.available = 1;
  }
  else
  {
    printf("Measurement Name not yet supported\n");
  }
}

typedef void (*log_meas_value)(byte_array_t name, meas_record_lst_t meas_record);

static log_meas_value get_meas_value[END_MEAS_VALUE] = {
    log_int_value,
    log_real_value,
    NULL,
};

static void match_meas_name_type(meas_type_t meas_type, meas_record_lst_t meas_record)
{
  if ((size_t)meas_record.value >= END_MEAS_VALUE ||
      get_meas_value[meas_record.value] == NULL)
  {
    observation.incomplete = 1;
    return;
  }
  get_meas_value[meas_record.value](meas_type.name, meas_record);
}

static void match_id_meas_type(meas_type_t meas_type, meas_record_lst_t meas_record)
{
  (void)meas_type;
  (void)meas_record;
  observation.incomplete = 1;
}

typedef void (*check_meas_type)(meas_type_t meas_type, meas_record_lst_t meas_record);

static check_meas_type match_meas_type[END_MEAS_TYPE] = {
    match_meas_name_type,
    match_id_meas_type,
};

static void log_kpm_measurements(kpm_ind_msg_format_1_t const *msg_frm_1)
{
  if (msg_frm_1 == NULL || msg_frm_1->meas_info_lst_len == 0)
  {
    observation.incomplete = 1;
    return;
  }

  // UE Measurements per granularity period
  for (size_t j = 0; j < msg_frm_1->meas_data_lst_len; j++)
  {
    meas_data_lst_t const data_item = msg_frm_1->meas_data_lst[j];

    size_t record_count = data_item.meas_record_len;
    if (record_count != msg_frm_1->meas_info_lst_len)
    {
      observation.incomplete = 1;
      if (record_count > msg_frm_1->meas_info_lst_len)
        record_count = msg_frm_1->meas_info_lst_len;
    }

    if (data_item.incomplete_flag && *data_item.incomplete_flag == TRUE_ENUM_VALUE)
    {
      observation.incomplete = 1;
      printf("Measurement Record not reliable\n");
    }

    for (size_t z = 0; z < record_count; z++)
    {
      meas_type_t const meas_type = msg_frm_1->meas_info_lst[z].meas_type;
      meas_record_lst_t const record_item = data_item.meas_record_lst[z];

      if ((size_t)meas_type.type < END_MEAS_TYPE &&
          match_meas_type[meas_type.type] != NULL)
      {
        match_meas_type[meas_type.type](meas_type, record_item);
      }
      else
      {
        observation.incomplete = 1;
      }
    }
  }
}

static void sm_cb_kpm(sm_ag_if_rd_t const *rd)
{
  if (rd == NULL || rd->type != INDICATION_MSG_AGENT_IF_ANS_V0 ||
      rd->ind.type != KPM_STATS_V3_0)
  {
    fprintf(stderr, "Ignoring an unexpected KPM callback payload\n");
    return;
  }

  kpm_ind_data_t const *ind = &rd->ind.kpm.ind;
  kpm_ric_ind_hdr_format_1_t const *hdr_frm_1 = &ind->hdr.kpm_ric_ind_hdr_format_1;
  kpm_ind_msg_format_3_t const *msg_frm_3 = &ind->msg.frm_3;

  int64_t const now = time_now_us();
  static int counter = 1;

  {
    lock_guard(&mtx);

    long long receive_time_us = (long long)now;
    long long collect_start_time_raw = (long long)hdr_frm_1->collectStartTime;
    long long collect_start_time_us = -1;
    long long latency_us = -1;

    if (collect_start_time_raw > 0 && collect_start_time_raw < 1000000000000LL)
    {
      collect_start_time_us = collect_start_time_raw * 1000000LL;
    }
    else
    {
      collect_start_time_us = collect_start_time_raw;
    }

    observation.kpm_indications_received++;
    observation.last_kpm_received_at_us = realtime_now_us();
    observation.last_kpm_collect_start_time = collect_start_time_raw;
    observation.incomplete = 0;
    observation.prb_tot_dl.available = 0;
    observation.prb_tot_ul.available = 0;
    observation.pdcp_volume_dl.available = 0;
    observation.pdcp_volume_ul.available = 0;
    observation.rlc_delay_dl.available = 0;
    observation.ue_throughput_dl.available = 0;
    observation.ue_throughput_ul.available = 0;
    if (msg_frm_3->ue_meas_report_lst_len == 0)
      observation.incomplete = 1;

    if (collect_start_time_us > 0 && collect_start_time_us <= receive_time_us)
    {
      latency_us = receive_time_us - collect_start_time_us;
      observation.last_kpm_latency_us = latency_us;
      observation.latency_available = 1;

      printf("\n%7d KPM ind_msg latency = %lld [us]\n", counter, latency_us);
    }
    else
    {
      observation.last_kpm_latency_us = -1;
      observation.latency_available = 0;

      printf("\n%7d KPM indication received; latency unavailable\n", counter);
    }

    for (size_t i = 0; i < msg_frm_3->ue_meas_report_lst_len; i++)
    {
      ue_id_e2sm_t const ue_id_e2sm = msg_frm_3->meas_report_per_ue[i].ue_meas_report_lst;
      ue_id_e2sm_e const type = ue_id_e2sm.type;
      if ((size_t)type < END_UE_ID_E2SM && log_ue_id_e2sm[type] != NULL)
        log_ue_id_e2sm[type](ue_id_e2sm);
      else
        observation.incomplete = 1;

      log_kpm_measurements(&msg_frm_3->meas_report_per_ue[i].ind_msg_format_1);
    }

    counter++;
  }
}

static test_info_lst_t filter_predicate(test_cond_type_e type, test_cond_e cond, int value)
{
  test_info_lst_t dst = {0};

  dst.test_cond_type = type;
  // It can only be TRUE_TEST_COND_TYPE so it does not matter the type
  // but ugly ugly...
  dst.S_NSSAI = TRUE_TEST_COND_TYPE;

  dst.test_cond = calloc(1, sizeof(test_cond_e));
  assert(dst.test_cond != NULL && "Memory exhausted");
  *dst.test_cond = cond;

  dst.test_cond_value = calloc(1, sizeof(test_cond_value_t));
  assert(dst.test_cond_value != NULL && "Memory exhausted");
  dst.test_cond_value->type = OCTET_STRING_TEST_COND_VALUE;

  dst.test_cond_value->octet_string_value = calloc(1, sizeof(byte_array_t));
  assert(dst.test_cond_value->octet_string_value != NULL && "Memory exhausted");
  const size_t len_nssai = 1;
  dst.test_cond_value->octet_string_value->len = len_nssai;
  dst.test_cond_value->octet_string_value->buf = calloc(len_nssai, sizeof(uint8_t));
  assert(dst.test_cond_value->octet_string_value->buf != NULL && "Memory exhausted");
  dst.test_cond_value->octet_string_value->buf[0] = value;

  return dst;
}

static label_info_lst_t fill_kpm_label(void)
{
  label_info_lst_t label_item = {0};

  label_item.noLabel = ecalloc(1, sizeof(enum_value_e));
  *label_item.noLabel = TRUE_ENUM_VALUE;

  return label_item;
}

static kpm_act_def_format_1_t fill_act_def_frm_1(ric_report_style_item_t const *report_item)
{
  assert(report_item != NULL);

  kpm_act_def_format_1_t ad_frm_1 = {0};

  size_t const sz = report_item->meas_info_for_action_lst_len;

  // [1, 65535]
  ad_frm_1.meas_info_lst_len = sz;
  ad_frm_1.meas_info_lst = calloc(sz, sizeof(meas_info_format_1_lst_t));
  assert(ad_frm_1.meas_info_lst != NULL && "Memory exhausted");

  for (size_t i = 0; i < sz; i++)
  {
    meas_info_format_1_lst_t *meas_item = &ad_frm_1.meas_info_lst[i];
    // 8.3.9
    // Measurement Name
    meas_item->meas_type.type = NAME_MEAS_TYPE;
    meas_item->meas_type.name = copy_byte_array(report_item->meas_info_for_action_lst[i].name);

    // [1, 2147483647]
    // 8.3.11
    meas_item->label_info_lst_len = 1;
    meas_item->label_info_lst = ecalloc(1, sizeof(label_info_lst_t));
    meas_item->label_info_lst[0] = fill_kpm_label();
  }

  // 8.3.8 [0, 4294967295]
  ad_frm_1.gran_period_ms = period_ms;

  // 8.3.20 - OPTIONAL
  ad_frm_1.cell_global_id = NULL;

#if defined KPM_V2_03 || defined KPM_V3_00
  // [0, 65535]
  ad_frm_1.meas_bin_range_info_lst_len = 0;
  ad_frm_1.meas_bin_info_lst = NULL;
#endif

  return ad_frm_1;
}

static kpm_act_def_t fill_report_style_4(ric_report_style_item_t const *report_item)
{
  assert(report_item != NULL);
  assert(report_item->act_def_format_type == FORMAT_4_ACTION_DEFINITION);

  kpm_act_def_t act_def = {.type = FORMAT_4_ACTION_DEFINITION};

  // Fill matching condition
  // [1, 32768]
  act_def.frm_4.matching_cond_lst_len = 1;
  act_def.frm_4.matching_cond_lst = calloc(act_def.frm_4.matching_cond_lst_len, sizeof(matching_condition_format_4_lst_t));
  assert(act_def.frm_4.matching_cond_lst != NULL && "Memory exhausted");
  // Filter connected UEs by S-NSSAI criteria
  test_cond_type_e const type = S_NSSAI_TEST_COND_TYPE; // CQI_TEST_COND_TYPE
  test_cond_e const condition = EQUAL_TEST_COND;        // GREATERTHAN_TEST_COND
  int const value = 1;
  act_def.frm_4.matching_cond_lst[0].test_info_lst = filter_predicate(type, condition, value);

  // Fill Action Definition Format 1
  // 8.2.1.2.1
  act_def.frm_4.action_def_format_1 = fill_act_def_frm_1(report_item);

  return act_def;
}

typedef kpm_act_def_t (*fill_kpm_act_def)(ric_report_style_item_t const *report_item);

static fill_kpm_act_def get_kpm_act_def[END_RIC_SERVICE_REPORT] = {
    NULL,
    NULL,
    NULL,
    fill_report_style_4,
    NULL,
};

static kpm_sub_data_t gen_kpm_subs(kpm_ran_function_def_t const *ran_func)
{
  kpm_sub_data_t kpm_sub = {0};
  if (ran_func == NULL || ran_func->ric_event_trigger_style_list == NULL ||
      ran_func->ric_report_style_list == NULL)
    return kpm_sub;

  // Generate Event Trigger
  if (ran_func->ric_event_trigger_style_list[0].format_type != FORMAT_1_RIC_EVENT_TRIGGER)
    return kpm_sub;
  kpm_sub.ev_trg_def.type = FORMAT_1_RIC_EVENT_TRIGGER;
  kpm_sub.ev_trg_def.kpm_ric_event_trigger_format_1.report_period_ms = period_ms;

  // Generate Action Definition
  kpm_sub.sz_ad = 1;
  kpm_sub.ad = calloc(kpm_sub.sz_ad, sizeof(kpm_act_def_t));
  if (kpm_sub.ad == NULL)
  {
    fprintf(stderr, "Could not allocate the KPM action definition\n");
    kpm_sub.sz_ad = 0;
    return kpm_sub;
  }

  // Multiple Action Definitions in one SUBSCRIPTION message is not supported in this project
  // Multiple REPORT Styles = Multiple Action Definition = Multiple SUBSCRIPTION messages
  ric_report_style_item_t *const report_item = &ran_func->ric_report_style_list[0];
  ric_service_report_e const report_style_type = report_item->report_style_type;
  if ((size_t)report_style_type >= END_RIC_SERVICE_REPORT ||
      get_kpm_act_def[report_style_type] == NULL)
  {
    free(kpm_sub.ad);
    kpm_sub.ad = NULL;
    kpm_sub.sz_ad = 0;
    return kpm_sub;
  }
  *kpm_sub.ad = get_kpm_act_def[report_style_type](report_item);

  return kpm_sub;
}

static bool eq_sm(sm_ran_function_t const *elem, int const id)
{
  if (elem->id == id)
    return true;

  return false;
}

static size_t find_sm_idx(sm_ran_function_t *rf, size_t sz, bool (*f)(sm_ran_function_t const *, int const), int const id)
{
  for (size_t i = 0; i < sz; i++)
  {
    if (f(&rf[i], id))
      return i;
  }

  return SIZE_MAX;
}

int main(int argc, char *argv[])
{
  fr_args_t args = init_fr_args(argc, argv);
  install_signal_handlers();
  signal(SIGPIPE, SIG_IGN);
  configure_publisher();

  CURLcode curl_init_result = curl_global_init(CURL_GLOBAL_DEFAULT);
  if (curl_init_result != CURLE_OK)
  {
    fprintf(stderr, "curl_global_init failed: %s\n", curl_easy_strerror(curl_init_result));
    return EXIT_FAILURE;
  }

  // Init the xApp
  init_xapp_api(&args);
  {
    lock_guard(&mtx);
    observation.ric_connected = 1;
  }

  pthread_t publisher_thread = {0};
  int publisher_started = pthread_create(
                              &publisher_thread, NULL, evidence_publisher_thread, NULL) == 0;
  if (!publisher_started)
  {
    fprintf(stderr, "Could not start the Evidence API publisher thread\n");
    stop_requested = 1;
  }

  // Wait for at least one E2 node instead of aborting. The publisher reports
  // zero nodes while the simulated RAN/FlexRIC environment is still starting.
  e2_node_arr_xapp_t nodes = {0};
  while (!stop_requested)
  {
    nodes = e2_nodes_xapp_api();
    {
      lock_guard(&mtx);
      observation.e2_nodes_connected = (int)nodes.len;
    }
    if (nodes.len > 0)
      break;

    if (nodes.n != NULL)
      free_e2_node_arr_xapp(&nodes);
    memset(&nodes, 0, sizeof(nodes));
    printf("Waiting for an E2 node...\n");
    for (int waited = 0; waited < 10 && !stop_requested; ++waited)
      usleep(100000);
  }

  printf("Connected E2 nodes = %d\n", (int)nodes.len);

  sm_ans_xapp_t *hndl = NULL;
  if (nodes.len > 0)
  {
    hndl = calloc(nodes.len, sizeof(sm_ans_xapp_t));
    if (hndl == NULL)
    {
      fprintf(stderr, "Could not allocate KPM subscription handles\n");
      stop_requested = 1;
    }
  }

  ////////////
  // START KPM
  ////////////
  int const KPM_ran_function = 2;

  for (size_t i = 0; i < nodes.len && !stop_requested; ++i)
  {
    e2_node_connected_xapp_t *n = &nodes.n[i];

    size_t const idx = find_sm_idx(n->rf, n->len_rf, eq_sm, KPM_ran_function);
    if (idx == SIZE_MAX || n->rf[idx].defn.type != KPM_RAN_FUNC_DEF_E)
    {
      fprintf(stderr, "E2 node does not expose the expected KPM RAN function\n");
      continue;
    }
    // if REPORT Service is supported by E2 node, send SUBSCRIPTION
    // e.g. OAI CU-CP
    if (n->rf[idx].defn.kpm.ric_report_style_list != NULL)
    {
      // Generate KPM SUBSCRIPTION message
      kpm_sub_data_t kpm_sub = gen_kpm_subs(&n->rf[idx].defn.kpm);
      if (kpm_sub.ad == NULL || kpm_sub.sz_ad == 0)
      {
        fprintf(stderr, "E2 node has no supported KPM report style\n");
        continue;
      }

      hndl[i] = report_sm_xapp_api(
          &n->id, KPM_ran_function, &kpm_sub, sm_cb_kpm);
      if (hndl[i].success != true)
        fprintf(stderr, "KPM subscription failed for an E2 node\n");
      free_kpm_sub_data(&kpm_sub);
    }
  }
  ////////////
  // END KPM
  ////////////

  printf("Health xApp is running continuously; press Ctrl+C to stop.\n");
  while (!stop_requested)
    usleep(200000);

  for (size_t i = 0; hndl != NULL && i < nodes.len; ++i)
  {
    // Remove the handle previously returned
    if (hndl[i].success == true)
      rm_report_sm_xapp_api(hndl[i].u.handle);
  }
  free(hndl);

  // Stop the xApp
  while (try_stop_xapp_api() == false)
    usleep(1000);

  if (publisher_started)
    pthread_join(publisher_thread, NULL);

  if (nodes.n != NULL)
    free_e2_node_arr_xapp(&nodes);
  {
    lock_guard(&mtx);
    observation.ric_connected = 0;
    observation.e2_nodes_connected = 0;
  }
  pthread_mutex_destroy(&mtx);
  curl_global_cleanup();

  printf("Health xApp stopped cleanly\n");
  return EXIT_SUCCESS;
}
