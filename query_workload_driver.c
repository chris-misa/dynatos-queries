/*
 * Independent libulfius-based query driver
 *
 * Adapted to execute randomly generated query workloads (e.g., from gen_query_workload.py).
 * The epoch id column in the output table corresponds to the queries index in the workload array
 */

#define DEBUG_STQL /* Flag to select if stql of generated queries should be printed */

#define DO_FOLLOWUP /* Flag to enable sending followup queries */

#define COOLDOWN_TIME 60.0 /* Time (sec) waited after sending all (base) queries before exiting */

#define MIN_FOLLOWUP_COUNT 10 /* Only select keys with >= MIN_FOLLOWUP_COUNT when selecting filters for followup queries */

#define START_ENDPOINT "http://localhost:8080/start" /* Endpoint where switch module is listening for new queries */
#define SWITCH_MODULE_STATUS "http://localhost:8080/status" /* Endpoint where switch module is listening for status (i.e., metadata) requests */

#define DEFAULT_PORT 9090
#define DEFAULT_ENDPOINT "/results"
#define RESULTS_ENDPOINT "localhost:%d/results"
#define ENDPOINT_STRLEN 128


#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <arpa/inet.h>

#include <ulfius.h>
#include <jansson.h>

#define QUERY_BUF_LEN 4096

#define BASE_QUERY_TEMPLATE "ingress.reduce({%s mask 0x%X}, {a = PktCount()}).export(periodic(1000))"
#define FOLLOWUP_QUERY_TEMPLATE "ingress.filter(%s == 0x%X mask 0x%X).reduce({%s mask 0x%X}, {a = PktCount()}).export(periodic(1000))"

void
usage()
{
    printf("Usage <query workload json file> <csv output file> <metadata json output file> [<recv port>]\n");
}

typedef struct {
    FILE *csv_out;
    FILE *metadata_out;
    char *query;
    json_t *options;
    int eid;
    json_t *query_metadata;
    char results_endpoint[ENDPOINT_STRLEN];
    int max_base_query;
    int followup_idx;
    json_t *workload;
} state_t;

void
state_t_init(state_t *s)
{
    s->csv_out = NULL;
    s->metadata_out = NULL;
    s->query = NULL;
    s->options = NULL;
    s->eid = 0;
    s->query_metadata = json_array();
    memset(s->results_endpoint, 0, ENDPOINT_STRLEN);
    s->max_base_query = 0;
    s->followup_idx = 0;
}

int
send_query(state_t *state, char *query, json_t *options, int id)
{
    struct _u_request req;
    struct _u_response resp;
    json_t *json_body = NULL;
    json_t *json_resp = NULL;
    int rv = 0;

#ifdef DEBUG_STQL
    printf("----- sending: \"%s\"\n", query);
#endif

    json_body = json_object();
    json_object_set_new(json_body, "id", json_integer(id));
    json_object_set_new(json_body, "stql", json_string(query));
    json_object_set_new(json_body, "url", json_string(state->results_endpoint));
    json_object_set(json_body, "options", options);

    ulfius_init_request(&req);

    req.http_verb = o_strdup("POST");
    req.http_url = o_strdup(START_ENDPOINT);
    req.timeout = 20;

    ulfius_set_json_body_request(&req, json_body);

    ulfius_init_response(&resp);
    rv = ulfius_send_http_request(&req, &resp);
    if (rv == U_OK) {
        printf("Sent request for query id %d\n", id);

    } else {
        fprintf(stderr, "Failed to send request\n");
        return -1;
    }

    json_resp = ulfius_get_json_body_response(&resp, NULL);
    printf("Response: ");
    json_dumpf(json_resp, stdout, 0);
    printf("\n");

    ulfius_clean_response(&resp);
    json_decref(json_body);
    ulfius_clean_request(&req);

    return 0;
}


#define JSON_KEY_SRC_IP "iana:sourceIPv4Address"
#define JSON_KEY_DST_IP "iana:destinationIPv4Address"
#define JSON_KEY_SRC_L4_PORT "iana:sourceTransportPort"
#define JSON_KEY_DST_L4_PORT "iana:destinationTransportPort"
#define JSON_KEY_IP_LENGTH "iana:totalLengthIPv4"
#define JSON_KEY_PACKET_COUNT "onrg:packetCount"
#define JSON_KEY_BYTE_COUNT "onrg:byteCount"

void
write_value_or_zero(FILE *fp, json_t *v, const char *key)
{
    json_t *tmp = json_object_get(v, key);
    if (tmp == NULL) {
        fprintf(fp, "0,");
    } else {
        switch (json_typeof(tmp)) {
            case JSON_STRING:
                fprintf(fp, "%s,", json_string_value(tmp));
                break;
            case JSON_INTEGER:
                fprintf(fp, "%d,", json_integer_value(tmp));
                break;
            case JSON_REAL:
                fprintf(fp, "%g,", json_real_value(tmp));
                break;
            default:
                fprintf(stderr, "Ignoring unsupported json type of key \"%s\"\n", key);
                break;
        }
    }
}

void
dump_to_csv(FILE *fp, json_t *res_array, int eid)
{
    size_t i;
    json_t *v;

    json_array_foreach(res_array, i, v) {
        write_value_or_zero(fp, v, JSON_KEY_SRC_IP);
        write_value_or_zero(fp, v, JSON_KEY_DST_IP);
        if (json_object_get(v, JSON_KEY_SRC_L4_PORT) != NULL) {
            write_value_or_zero(fp, v, JSON_KEY_SRC_L4_PORT);
        } else if (json_object_get(v, JSON_KEY_IP_LENGTH) != NULL) {
            write_value_or_zero(fp, v, JSON_KEY_IP_LENGTH);
        } else {
            fprintf(fp, "0,");
        }
        write_value_or_zero(fp, v, JSON_KEY_DST_L4_PORT);
        write_value_or_zero(fp, v, JSON_KEY_PACKET_COUNT);
        write_value_or_zero(fp, v, JSON_KEY_BYTE_COUNT);
        fprintf(fp, "%d\n", eid);
    }
}

json_t *
get_switch_module_metadata(state_t *state)
{
    struct _u_request req;
    struct _u_response resp;
    json_t *json_body = NULL;
    int rv = 0;

    ulfius_init_request(&req);

    req.http_verb = o_strdup("GET");
    req.http_url = o_strdup(SWITCH_MODULE_STATUS);
    req.timeout = 20;

    ulfius_init_response(&resp);
    rv = ulfius_send_http_request(&req, &resp);
    if (rv == U_OK) {
        printf("Send request for switch module metadata\n", state->eid);
        state->eid++;
    } else {
        fprintf(stderr, "Failed to send request\n");
        return NULL;
    }

    json_body = ulfius_get_json_body_response(&resp, NULL);

    ulfius_clean_response(&resp);
    ulfius_clean_request(&req);

    return json_body;
}

int
_get_filters(json_t *data, uint32_t *filters, int num_filters)
{
    int i;
    int idx = 0;
    json_t *rec;
    int count;
    json_t *val;
    uint32_t res;

    for (i = 0; i < num_filters; i++) {

        do {
            rec = json_array_get(data, idx);
            if (rec == NULL) {
                fprintf(stderr, "WARNING :: failed to pull a results from data for followup filter\n");
                return -1;
            }
            count = json_integer_value(json_object_get(rec, JSON_KEY_PACKET_COUNT));
            idx++;
        } while (count < MIN_FOLLOWUP_COUNT);


        val = json_object_get(rec, JSON_KEY_SRC_IP);
        if (val == NULL) {
            val = json_object_get(rec, JSON_KEY_DST_IP);
        }
        if (val == NULL) {
            fprintf(stderr, "WARNING :: failed to find usable key for followup filter\n");
            return -1;
        }
        inet_pton(AF_INET, json_string_value(val), &res);
        filters[i] = ntohl(res);
    }
    return 0;
}

int
_get_mask(int bits)
{
    return ((0xFFFFFFFF << (32 - bits)) & 0xFFFFFFFF);
}

void
send_followup_queries(state_t *state, int id, json_t *data)
{
    json_t *queries;
    json_t *orig;
    int gran;
    int num_followups;
    double sigma;
    int i;
    json_t *options;
    const char *key_field;
    char query_buf[QUERY_BUF_LEN] = {'\0'};

    queries = json_object_get(state->workload, "queries");
    if (queries == NULL) {
        fprintf(stderr, "WARNING :: state->workload[\"queries\"] is NULL!\n");
        return;
    }

    orig = json_array_get(queries, id);
    if (orig == NULL) {
        fprintf(stderr, "WARNING :: failed to find original query %d for followups\n", id);
        return;
    }

    gran = json_integer_value(json_object_get(orig, "init_gran")); /* <------ changed to use same granularity as base query */
    num_followups = json_integer_value(json_object_get(orig, "num_followups"));
    sigma = json_real_value(json_object_get(orig, "followup_sigma"));
    key_field = json_string_value(json_object_get(orig, "key_field"));

    options = json_object();
    json_object_set_new(options, "sigma", json_real(sigma));
    json_object_set_new(options, "target_num_epochs", json_integer(1));

    printf("(following up for query %d with %d follow ups at %d bits, sigma = %f)\n", id, num_followups, gran, sigma);

    uint32_t filters[num_followups];
    memset(filters, 0, sizeof(uint32_t) * num_followups);
    if (_get_filters(data, filters, num_followups) != 0) {
        fprintf(stderr, "WARNING :: failed to get filters for follow ups to query id %d\n", id);
        return;
    }

    for (i = 0; i < num_followups; i++) {

        snprintf(query_buf, QUERY_BUF_LEN, FOLLOWUP_QUERY_TEMPLATE,
            key_field, filters[i], _get_mask(gran),
            key_field, _get_mask(gran));
        printf("Sending follow up query: \"%s\"\n", query_buf);

        send_query(state, query_buf, options, state->followup_idx++);
    }

    json_decref(options);
}

int
main_callback(const struct _u_request *req, struct _u_response *resp, void *user_data)
{
    state_t *state = (state_t *)user_data;
    json_t *json_in = ulfius_get_json_body_request(req, NULL);
    json_t *data = NULL;
    json_t *metadata = NULL;
    int eid = 0;
    int rv = 0;

    eid = json_integer_value(json_object_get(json_in, "id"));
    data = json_object_get(json_in, "data");
    if (data == NULL) {
        printf(". . . failed to find \"data\" key\n");
    } else {
        printf(". . . received %lu results\n", json_array_size(data));
        dump_to_csv(state->csv_out, data, eid);
    }
   
    metadata = json_object_get(json_in, "metadata");
    if (metadata == NULL) {
        fprintf(stderr, ". . . failed to find \"metadata\" key\n");
    } else {
        json_object_set_new(metadata, "id", json_integer(eid));
        json_array_append(state->query_metadata, metadata);
    }

    /* Check for follow up queries */
#ifdef DO_FOLLOWUP
    if (eid < state->max_base_query) {
        send_followup_queries(state, eid, data);
    }
#endif

    fflush(state->csv_out);

    json_decref(json_in);

    return U_CALLBACK_CONTINUE;
}

/* Wrapper around nanosleep */
static inline void
_nanosleep(struct timespec *dur)
{
    struct timespec remain;
    memcpy(&remain, dur, sizeof(remain));

    while (nanosleep(&remain, &remain) != 0 && errno == EINTR) {
    }
}

/* Wrapper to sleep a number of seconds given as a double */
static inline void
_dsleep(double secs)
{
    struct timespec dur;

    dur.tv_sec = (long long unsigned)secs;
    dur.tv_nsec = (long long unsigned)((secs - (long long unsigned)secs) * 1000000000);
    _nanosleep(&dur);
}


int
workload_send_loop(state_t *state)
{
    int rv = 0;
    size_t idx;
    json_t *q;
    json_t *queries;
    double dt;
    const char *key_field;
    int init_gran;
    double sigma;
    int num_epochs;
    char query_buf[QUERY_BUF_LEN] = {'\0'};
    json_t *workload = state->workload;
    
    queries = json_object_get(workload, "queries");
    if (queries == NULL) {
        fprintf(stderr, "Failed to find \"queries\" field on workload object...\n");
        rv = -1;
        goto done;
    }

    printf("Send Loop :: sending %d queries according to schedule...\n", json_array_size(queries));
    state->max_base_query = json_array_size(queries) + 100;
    state->followup_idx = state->max_base_query;

    json_array_foreach(queries, idx, q) {

        printf("Preparing base query %d\n", idx);
        json_t *options = json_object();

        dt = json_real_value(json_object_get(q, "dt"));
        key_field = json_string_value(json_object_get(q, "key_field"));
        init_gran = json_integer_value(json_object_get(q, "init_gran"));
        sigma = json_real_value(json_object_get(q, "sigma"));
        num_epochs = json_integer_value(json_object_get(q, "num_epochs"));


        snprintf(query_buf, QUERY_BUF_LEN, BASE_QUERY_TEMPLATE, key_field, _get_mask(init_gran));
    
        json_object_set_new(options, "sigma", json_real(sigma));
        json_object_set_new(options, "target_num_epochs", json_integer(num_epochs));

        _dsleep(dt);

        printf("Sending base query %d\n", idx);
        rv = send_query(state, query_buf, options, idx);
        if (rv != 0) {
            fprintf(stderr, "Failed to send query...\n");
            goto done;
        }

        json_decref(options);
    }

done:
    return rv;
}

int
main(int argc, char *argv[])
{
    /* Load arguments */
    if (argc != 4 && argc != 5) {
        usage();
        return 0;
    }
    char *query_workload_path = argv[1];
    char *output_filepath = argv[2];
    char *metadata_filepath = argv[3];
    int results_port = DEFAULT_PORT;
    
    if (argc == 5) {
        results_port = atoi(argv[4]);
    }

    struct _u_instance ulfius_instance;
    state_t *state = NULL;
    int rv = 0;
    json_t *query_workload = NULL;
    FILE *query_workload_file = NULL;

    query_workload_file = fopen(query_workload_path, "r");
    if (query_workload_file == NULL) {
        fprintf(stderr, "Failed to open query workload file at \"%s\"\n", query_workload_path);
        return -1;
    }

    query_workload = json_loadf(query_workload_file, 0, NULL);
    if (query_workload == NULL) {
        fprintf(stderr, "Failed to parse \"%s\" as json\n", query_workload_path);
        return -1;
    }
    fclose(query_workload_file);
    query_workload_file = NULL;


    /* Allocate and init state object */
    state = (state_t *)malloc(sizeof(*state));
    if (state == NULL) {
        fprintf(stderr, "No memory for state struct\n");
        return -1;
    }
    state_t_init(state);

    snprintf(state->results_endpoint, ENDPOINT_STRLEN,
            RESULTS_ENDPOINT, results_port);

    /* Open output files */
    state->csv_out = fopen(output_filepath, "w");
    if (state->csv_out == NULL) {
        fprintf(stderr, "Failed to open \"%s\" for output\n", argv[1]);
        return -1;
    }
    state->metadata_out = fopen(metadata_filepath, "w");
    if (state->metadata_out == NULL) {
        fprintf(stderr, "Failed to open \"%s\" for output\n", argv[2]);
        return -1;
    }

    /* Start ulfius server with results endpoint */
    if (ulfius_init_instance(&ulfius_instance, results_port, NULL, NULL) != U_OK) {
        fprintf(stderr, "CSV Listener :: failed to init ulfius interface\n");
        return -1;
    }
    ulfius_add_endpoint_by_val(&ulfius_instance, "POST", DEFAULT_ENDPOINT, NULL, 0, &main_callback, state);

    if (ulfius_start_framework(&ulfius_instance) != U_OK) {
        fprintf(stderr, "CSV Listener :: failed to start ulfius instance\n");
        return -1;
    }

    printf("CSV Listener :: listening on %s\n", state->results_endpoint);


    /* Enter workload sending loop */
    state->workload = query_workload;
    if (workload_send_loop(state) != 0) {
        fprintf(stderr, "WARNING :: workload sending loop failed\n");
    }

    /* Wait a bit for previously submitted queries */
    _dsleep(COOLDOWN_TIME);

    printf("Reading switch module metadata\n");
    json_t *final_meta = json_object();
    json_object_set(final_meta, "queries", state->query_metadata);

    json_t *sm_meta = get_switch_module_metadata(state);
    if (sm_meta == NULL) {
        fprintf(stderr, ". . . failed to get switch module metadata!\n");
    } else {
        json_object_set_new(final_meta, "switch_module", sm_meta);
    }

    json_dumpf(final_meta, state->metadata_out, JSON_INDENT(2));
    fprintf(state->metadata_out, "\n");

    printf("CSV Listener :: exiting\n");

    ulfius_stop_framework(&ulfius_instance);
    ulfius_clean_instance(&ulfius_instance);

    fflush(state->csv_out);
    fflush(state->metadata_out);

    free(state);
    
    return 0;
}
