#include <alloca.h>
#include <inttypes.h>
#include </home/matthew/janet/janet.h>
#include <stdio.h>
#include <mysql/mysql.h>
#include <string.h>

static Janet safe_ckeywordv(const char *s) {
    return s ? janet_ckeywordv(s) : janet_wrap_nil();
}

static void stmt_panic(MYSQL_STMT *statement, const char *where) {
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s failed: code=%d error=%s", where, mysql_stmt_errno(statement), mysql_stmt_error(statement));
    janet_panic(buf);
}

static void conn_panic(MYSQL *conn, const char *where) {
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s failed: code=%d error=%s", where, mysql_errno(conn), mysql_error(conn));
    janet_panic(buf);
}

typedef struct {
    unsigned long long affected_rows;
    unsigned long long insert_id;
} jmy_result_t;

static int result_gc(void *p, size_t s) {
    (void)p;
    (void)s;
    return 0;
}

static void result_to_string(void *p, JanetBuffer *buffer) {
    jmy_result_t *result = (jmy_result_t *)p;

    janet_buffer_push_cstring(buffer, "result: ");
    char buf[1024];
    sprintf(buf, "insert_id %llu affected_rows %llu", result->insert_id, result->affected_rows);
    janet_buffer_push_cstring(buffer, buf);
}

static const JanetAbstractType result_type = {
    "mysql/result",
    result_gc,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    result_to_string,
    NULL,
    NULL,
    NULL,
    NULL
};

static Janet result_insert_id(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_result_t *result = (jmy_result_t *)janet_getabstract(argv, 0, &result_type);
    return janet_wrap_number(result->insert_id);
}

static Janet result_affected_rows(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_result_t *result = (jmy_result_t *)janet_getabstract(argv, 0, &result_type);
    return janet_wrap_number(result->affected_rows);
}

typedef struct {
    MYSQL_STMT *statement;
    MYSQL_RES *r;
    int num_fields;
} jmy_rows_t;

static void __ensure_rows_ok(jmy_rows_t *ctx) {
    if (ctx->r == NULL) {
        janet_panic("mysql/rows is disconnected");
    }
}

static int rows_gc(void *p, size_t s) {
    (void)s;
    jmy_rows_t *rows = (jmy_rows_t *)p;
    if (rows->r) {
        mysql_free_result(rows->r);
        rows->r = NULL;
    }
    return 0;
}

static void rows_to_string(void *p, JanetBuffer *buffer) {
    jmy_rows_t *rows = (jmy_rows_t *)p;
    __ensure_rows_ok(rows);

    janet_buffer_push_cstring(buffer, "result: ");
    if (rows->r) {
        janet_buffer_push_cstring(buffer, "...");
    }
}

static const JanetAbstractType rows_type = {
    "mysql/rows",
    rows_gc,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    rows_to_string,
    NULL,
    NULL,
    NULL,
    NULL
};

static Janet rows_columns(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_rows_t *rows = (jmy_rows_t *)janet_getabstract(argv, 0, &rows_type);
    __ensure_rows_ok(rows);

    int n = mysql_num_fields(rows->r);
    MYSQL_FIELD *mysqlFields = mysql_fetch_fields(rows->r);
    JanetArray *a = janet_array(n);
    for (int i = 0; i < n; i++) {
        janet_array_push(a, janet_wrap_string(mysqlFields[i].name));
    }

    return janet_wrap_array(a);
}

static Janet rows_column_types(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_rows_t *rows = (jmy_rows_t *)janet_getabstract(argv, 0, &rows_type);
    __ensure_rows_ok(rows);
    int n = mysql_num_fields(rows->r);
    MYSQL_FIELD *mysqlFields = mysql_fetch_fields(rows->r);
    JanetArray *a = janet_array(n);
    for (int i = 0; i < n; i++) {
        janet_array_push(a, janet_wrap_number(mysqlFields[i].type));
    }

    return janet_wrap_array(a);
}

typedef struct {
    MYSQL_STMT *statement;
} jmy_statement_t;

static void __ensure_stmt_ok(jmy_statement_t *stmt) {
    if (stmt->statement == NULL) {
        janet_panic("mysql/statement is closed");
    }
}

static int statement_gc(void *p, size_t s) {
    (void)s;
    //printf("rows gc %p\n", p);
    jmy_statement_t *rows = (jmy_statement_t *)p;
    if (rows->statement) {
        mysql_stmt_close(rows->statement);
        rows->statement = NULL;
    }
    return 0;
}

static void statement_to_string(void *p, JanetBuffer *buffer) {
    jmy_statement_t *stmt = (jmy_statement_t *)p;
    (void)stmt;

    janet_buffer_push_cstring(buffer, "statement: ");
}

static const JanetAbstractType statement_type = {
    "mysql/statement",
    statement_gc,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    statement_to_string,
    NULL,
    NULL,
    NULL,
    NULL
};

typedef struct {
    MYSQL *conn;
    bool in_transaction;
} jmy_context_t;

static void __ensure_ctx_ok(jmy_context_t *ctx) {
    if (ctx->conn == NULL) {
        janet_panic("mysql/context is disconnected");
    }
}

static void context_close_i(jmy_context_t *ctx) {
    if (ctx->conn) {
        mysql_close(ctx->conn);
        ctx->conn = NULL;
    }
}

static int context_gc(void *p, size_t s) {
    (void)s;
    jmy_context_t *ctx = (jmy_context_t *)p;
    context_close_i(ctx);
    return 0;
}

static Janet context_close(int32_t argc, Janet *argv);

static JanetMethod context_methods[] = {
    {"close", context_close}, /* So contexts can be used with 'with' */
    {NULL, NULL}
};

static int context_get(void *ptr, Janet key, Janet *out) {
    (void)ptr;
    return janet_getmethod(janet_unwrap_keyword(key), context_methods, out);
}

static const JanetAbstractType context_type = {
    "mysql/context",
    context_gc,
    NULL,
    context_get,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};

static Janet context_connect(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    // The configuration data is provided as a struct:
    // :host
    // :username
    // :password
    // :database
    JanetStruct config = janet_getstruct(argv, 0);
    Janet host = janet_struct_get(config, janet_ckeywordv("host"));
    if (!janet_checktype(host, JANET_STRING)) {
        janet_panicf("host is not a string");
    }
    Janet username = janet_struct_get(config, janet_ckeywordv("username"));
    if (!janet_checktype(username, JANET_STRING)) {
        janet_panicf("username is not a string");
    }
    Janet password = janet_struct_get(config, janet_ckeywordv("password"));
    if (janet_checktype(password, JANET_NIL)) {
        password = janet_wrap_string("");
    }
    if (!janet_checktype(password, JANET_STRING))
        janet_panicf("password is not a string");
    Janet database = janet_struct_get(config, janet_ckeywordv("database"));
    if (janet_checktype(database, JANET_NIL)) {
        database = janet_wrap_string("");
    }
    if (!janet_checktype(database, JANET_STRING)) {
        janet_panicf("database is not a string");
    }

    jmy_context_t *ctx = (jmy_context_t *)janet_abstract(&context_type, sizeof(jmy_context_t));

    MYSQL *conn = mysql_init(NULL);

    // Unpack connection parameters.
    if (mysql_real_connect(conn,
                           (const char *)janet_unwrap_string(host), (const char *)janet_unwrap_string(username),
                           (const char *)janet_unwrap_string(password), (const char *)janet_unwrap_string(database), 0, NULL, 0) == NULL) {
        janet_panic("unable to create connection");
    }

    ctx->conn = conn;
    ctx->in_transaction = false;

    return janet_wrap_abstract(ctx);
}

static Janet context_select_db(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);
    const char *db = janet_getcstring(argv, 1);
    if (mysql_select_db(ctx->conn, db)) {
        conn_panic(ctx->conn, "mysql_select_db");
    }

    return janet_wrap_nil();
}

static Janet context_prepare(int32_t argc, Janet *argv) {
    if (argc < 2) {
        janet_panic("expected at least a pq context and a query string");
    }
    if (argc > 10000000) {
        janet_panic("too many arguments");
    }

    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);

    const char *q = janet_getcstring(argv, 1);
    int len = strlen(q);

    argc -= 2;
    argv += 2;

    //puts("init");
    MYSQL_STMT *statement = mysql_stmt_init(ctx->conn);
    if (statement == NULL) {
        conn_panic(ctx->conn, "mysql_stmt_init");
    }

    if (mysql_stmt_prepare(statement, q, len)) {
        mysql_stmt_close(statement);
        conn_panic(ctx->conn, "mysql_stmt_prepare");
    }

    jmy_statement_t *result = (jmy_statement_t *)janet_abstract(&statement_type, sizeof(jmy_statement_t));
    result->statement = statement;

    return janet_wrap_abstract(result);
}

typedef struct {
    MYSQL_BIND *binds;
    bool *nulls;
    unsigned long *lengths;
    bool *errors;
    int len;
} jmy_query_bind_t;

static void query_bind_free(jmy_query_bind_t binds) {
    if (binds.binds == NULL) {
        return;
    }

    for (int i = 0; i < binds.len; i++) {
        janet_sfree(binds.binds[i].buffer);
    }
    janet_sfree(binds.nulls);
    janet_sfree(binds.lengths);
    janet_sfree(binds.errors);
}

static jmy_query_bind_t allocate_binds(int num_fields, MYSQL_FIELD *fields) {
    MYSQL_BIND *binds = (MYSQL_BIND *)janet_smalloc(sizeof(MYSQL_BIND) * num_fields);
    memset(binds, 0, sizeof(MYSQL_BIND) * num_fields);

    bool *nulls = (bool *)janet_smalloc(sizeof(bool) * num_fields);
    memset(nulls, 0, sizeof(bool) * num_fields);

    unsigned long *lengths = janet_smalloc(sizeof(unsigned long) * num_fields);
    memset(lengths, 0, sizeof(unsigned long) * num_fields);

    bool *errors = janet_smalloc(sizeof(bool) * num_fields);
    memset(errors, 0, sizeof(bool) * num_fields);

    for (int i = 0; i < num_fields; ++i) {
        unsigned long len = 0;
        switch (fields[i].type) {
            case MYSQL_TYPE_TINY:
                len = 1;
                break;

            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_YEAR:
                len = 2;
                break;

            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_LONG:
                len = 4;
                break;

            case MYSQL_TYPE_LONGLONG:
                len = 8;
                break;

            case MYSQL_TYPE_FLOAT:
                len = 4;
                break;

            case MYSQL_TYPE_DOUBLE:
                len = 8;
                break;

            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TIMESTAMP2:
            case MYSQL_TYPE_DATETIME:
                len = sizeof(MYSQL_TIME);
                break;

            case MYSQL_TYPE_JSON:
            case MYSQL_TYPE_NEWDECIMAL:
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_BIT:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
                len = fields[i].max_length;
                break;

            default:
                janet_panicf("unknown field type %d\n", fields[i].type);
        }

        binds[i].buffer_type = fields[i].type;
        binds[i].buffer = janet_smalloc(len);
        memset(binds[i].buffer, 0, len);
        binds[i].buffer_length = len;
        binds[i].is_null = &nulls[i];
        binds[i].length = &lengths[i];
        binds[i].error = &errors[i];
    }
    jmy_query_bind_t b = {binds, nulls, lengths, errors, num_fields};
    return b;
}

typedef struct {
    MYSQL_BIND *binds;
    unsigned long *lengths;
    int len;
} jmy_exec_bind_t;

static void exec_bind_free(jmy_exec_bind_t binds) {
    if (binds.binds == NULL) {
        return;
    }
    for (int i = 0; i < binds.len; i++) {
        switch (binds.binds[i].buffer_type) {
            case MYSQL_TYPE_NULL:
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_DOUBLE:
                janet_sfree(binds.binds[i].buffer);
                break;
            default:
                break;
        }
    }
    janet_sfree(binds.lengths);
    janet_sfree(binds.binds);
}

static jmy_exec_bind_t create_exec_bind(int32_t argc, Janet *argv) {
    if (argc == 0) {
        jmy_exec_bind_t t = { NULL, NULL, 0};
        return t;
    }

    MYSQL_BIND *binds = (MYSQL_BIND *)janet_smalloc(argc * sizeof(MYSQL_BIND));
    memset(binds, 0, argc * sizeof(MYSQL_BIND));

    unsigned long *lengths = (unsigned long *)janet_smalloc(sizeof(unsigned long) * argc);
    memset(lengths, 0, argc * sizeof(unsigned long));

    for (int i = 0; i < argc; i++) {
        Janet j = argv[i];
        switch (janet_type(j)) {
            case JANET_NIL: {
                binds[i].buffer_type = MYSQL_TYPE_NULL;
                bool *v = (bool *)janet_smalloc(sizeof(bool));
                *v = true;
                binds[i].is_null = v;
                break;
            }
            case JANET_BOOLEAN: {
                binds[i].buffer_type = MYSQL_TYPE_TINY;
                bool *v = (bool *)janet_smalloc(sizeof(bool));
                *v = janet_unwrap_boolean(j);
                binds[i].buffer = v;
                break;
            }
            case JANET_BUFFER: {
                binds[i].buffer_type = MYSQL_TYPE_STRING;
                JanetBuffer *b = janet_unwrap_buffer(j);
                binds[i].buffer = b->data;
                lengths[i] = b->count;
                binds[i].length = &lengths[i];
                break;
            }
            case JANET_KEYWORD:
            case JANET_STRING: {
                binds[i].buffer_type = MYSQL_TYPE_STRING;
                const char *s = (const char *)janet_unwrap_string(j);
                binds[i].buffer = (void *)s;
                lengths[i] = janet_string_length(s);
                binds[i].length = &lengths[i];
                break;
            }
            case JANET_NUMBER: {
                binds[i].buffer_type = MYSQL_TYPE_DOUBLE;
                double *v = janet_smalloc(sizeof(double));
                *v = janet_unwrap_number(j);
                binds[i].buffer = v;
                break;
            }

            case JANET_ABSTRACT: {
                JanetIntType intt = janet_is_int(j);
                if (intt == JANET_INT_S64 || intt == JANET_INT_U64) {
                    binds[i].buffer_type = MYSQL_TYPE_LONGLONG;
                    long long *v = janet_smalloc(sizeof(long long));
                    if (intt == JANET_INT_S64) {
                        *v = janet_unwrap_s64(j);
                    } else {
                        *v = (long long)janet_unwrap_u64(j);
                    }
                    binds[i].buffer = v;
                    break;
                }
                break;
            }
            /* fall-thru */

            default:
                janet_panicf("cannot encode janet type %d", janet_type(j));
        }
    }
    jmy_exec_bind_t b = {
        binds,
        lengths,
        argc,
    };
    return b;
}

static Janet stmt_exec(jmy_statement_t *stmt, int32_t argc, Janet *argv) {
    MYSQL_STMT *statement = stmt->statement;
    unsigned long param_count = mysql_stmt_param_count(statement);
    if ((unsigned long)argc != param_count) {
        janet_panicf("query: wrong arity %d expected got %d\n", param_count, argc);
    }

    jmy_exec_bind_t binds = create_exec_bind(argc, argv);
    if (binds.binds != NULL) {
        if (mysql_stmt_bind_param(statement, binds.binds)) {
            stmt_panic(statement, "mysql_stmt_bind_param");
        }
    }

    if (mysql_stmt_execute(statement)) {
        stmt_panic(statement, "mysql_stmt_execute");
    }
    exec_bind_free(binds);

    int num_fields = mysql_stmt_field_count(statement);
    if (num_fields > 0) {
        janet_panicf("field_count is %d not zero", num_fields);
    }
    jmy_result_t *result = (jmy_result_t *)janet_abstract(&result_type, sizeof(jmy_result_t));
    result->affected_rows = mysql_stmt_affected_rows(statement);
    result->insert_id = mysql_stmt_insert_id(statement);

    return janet_wrap_abstract(result);
}

static Janet stmt_select(jmy_statement_t *stmt, int32_t argc, Janet *argv) {
    argc -= 1;
    argv += 1;

    MYSQL_STMT *statement = stmt->statement;

    unsigned long param_count = mysql_stmt_param_count(statement);
    if ((unsigned long)argc != param_count) {
        janet_panicf("query: wrong arity %d expected got %d\n", param_count, argc);
    }

    jmy_exec_bind_t binds = create_exec_bind(argc, argv);
    if (binds.binds != NULL) {
        if (mysql_stmt_bind_param(statement, binds.binds)) {
            stmt_panic(statement, "mysql_stmt_bind_param");
        }
    }
    if (mysql_stmt_execute(statement)) {
        stmt_panic(statement, "mysql_stmt_execute");
    }
    exec_bind_free(binds);

    /* the column count is > 0 if there is a result set */
    /* 0 if the result is only the final status packet */
    int num_fields = mysql_stmt_field_count(statement);
    if (num_fields == 0) {
        janet_panicf("unexpected field_count is %d not zero", num_fields);
    }

    bool truth = 1;
    if (mysql_stmt_attr_set(statement, STMT_ATTR_UPDATE_MAX_LENGTH, &truth) != 0) {
        stmt_panic(statement, "mysql_stmt_attr_set");
    }

    if (mysql_stmt_store_result(statement)) {
        stmt_panic(statement, "mysql_stmt_store_result");
    }

    MYSQL_RES *r = mysql_stmt_result_metadata(statement);
    if (!r) {
        stmt_panic(statement, "mysql_stmt_result_metadata");
    }

    jmy_rows_t *rows = (jmy_rows_t *)janet_abstract(&rows_type, sizeof(jmy_rows_t));
    rows->statement = statement;
    rows->num_fields = num_fields;
    rows->r = r;

    return janet_wrap_abstract(rows);
}

static Janet stmt_close(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_statement_t *stmt = (jmy_statement_t *)janet_getabstract(argv, 0, &statement_type);
    __ensure_stmt_ok(stmt);
    if (stmt->statement != NULL) {
        mysql_stmt_close(stmt->statement);
        stmt->statement = NULL;
    }
    return janet_wrap_nil();
}

static Janet decode_text(char *v, unsigned long l, MYSQL_FIELD *field) {
    if (v == NULL) {
        return janet_wrap_nil();
    }

    Janet jv;
    switch (field->type) {
        case MYSQL_TYPE_NULL:
            jv = janet_wrap_nil();
            break;

        case MYSQL_TYPE_TINY: {
            int i = atoi(v);
            if (field->length == 1) {
                jv = janet_wrap_boolean(i != 0);
            } else {
                jv = janet_wrap_number(i);
            }
            break;
        }

        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_YEAR: {
            int i = atoi(v);
            jv = janet_wrap_number(i);
            break;
        }

        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG: {
            long i = atol(v);
            jv = janet_wrap_number(i);
            break;
        }

        case MYSQL_TYPE_FLOAT: {
            float i = atol(v);
            jv = janet_wrap_number(i);
            break;
        }

        case MYSQL_TYPE_DOUBLE: {
            double i = atof(v);
            jv = janet_wrap_number(i);
            break;
        }


        case MYSQL_TYPE_LONGLONG: {
            long long int i = atoll(v);
            jv = janet_wrap_number(i);
            break;
        }

        case MYSQL_TYPE_DATE: {
            MYSQL_TIME t;
            memset(&t, 0, sizeof(MYSQL_TIME));
            sscanf(v, "%d-%d-%d", &t.year, &t.month, &t.day);

            JanetKV *st = janet_struct_begin(3);
            janet_struct_put(st, janet_ckeywordv("day"), janet_wrap_number(t.day));
            janet_struct_put(st, janet_ckeywordv("month"), janet_wrap_number(t.month));
            janet_struct_put(st, janet_ckeywordv("year"), janet_wrap_number(t.year));
            jv = janet_wrap_struct(janet_struct_end(st));
            break;
        }
        case MYSQL_TYPE_TIME: {
            MYSQL_TIME t;
            memset(&t, 0, sizeof(MYSQL_TIME));
            sscanf(v, "%d:%d:%d", &t.hour, &t.minute, &t.second);

            JanetKV *st = janet_struct_begin(3);
            janet_struct_put(st, janet_ckeywordv("seconds"), janet_wrap_number(t.second));
            janet_struct_put(st, janet_ckeywordv("minutes"), janet_wrap_number(t.minute));
            janet_struct_put(st, janet_ckeywordv("hours"), janet_wrap_number(t.hour));
            jv = janet_wrap_struct(janet_struct_end(st));
            break;
        }
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP2: {
            MYSQL_TIME t;
            memset(&t, 0, sizeof(MYSQL_TIME));
            if (l == 19) {
                sscanf(v, "%d-%d-%d %d:%d:%d", &t.year, &t.month, &t.day, &t.hour, &t.minute, &t.second);
            } else {
                sscanf(v, "%d-%d-%d %d:%d:%d.%ld", &t.year, &t.month, &t.day, &t.hour, &t.minute, &t.second, &t.second_part);
            }
            int count;
            if (field->type == MYSQL_TYPE_DATETIME) {
                count = 8;
            } else {
                count = 7;
            }
            JanetKV *st = janet_struct_begin(count);
            janet_struct_put(st, janet_ckeywordv("microseconds"), janet_wrap_number(t.second_part));
            janet_struct_put(st, janet_ckeywordv("seconds"), janet_wrap_number(t.second));
            janet_struct_put(st, janet_ckeywordv("minutes"), janet_wrap_number(t.minute));
            janet_struct_put(st, janet_ckeywordv("hours"), janet_wrap_number(t.hour));
            janet_struct_put(st, janet_ckeywordv("day"), janet_wrap_number(t.day));
            janet_struct_put(st, janet_ckeywordv("month"), janet_wrap_number(t.month));
            janet_struct_put(st, janet_ckeywordv("year"), janet_wrap_number(t.year));
            if (field->type == MYSQL_TYPE_DATETIME) {
                janet_struct_put(st, janet_ckeywordv("tz"), janet_wrap_number(t.time_zone_displacement));
            }
            jv = janet_wrap_struct(janet_struct_end(st));
            break;
        }
        //MYSQL_TYPE_ENUM = 247,
        //MYSQL_TYPE_SET = 248,
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            jv = janet_wrap_string(janet_string((uint8_t *)v, l));
            break;
        //MYSQL_TYPE_GEOMETRY = 255

        default:
            janet_panicf("unexpected mysql type %d\n", field->type);
    }
    return jv;
}

static Janet rows_text_unpack(jmy_rows_t *rows) {
    int n = mysql_num_rows(rows->r);
    int num_fields = mysql_num_fields(rows->r);

    MYSQL_FIELD *fields = mysql_fetch_fields(rows->r);

    JanetArray *a = janet_array(n);
    for (int i = 0; i < n; i++) {
        JanetTable *t = janet_table(num_fields);
        MYSQL_ROW row = mysql_fetch_row(rows->r);
        unsigned long *lengths = mysql_fetch_lengths(rows->r);
        for (int j = 0; j < num_fields; j++) {
            Janet jv = decode_text(row[j], lengths[j], &fields[j]);
            Janet k = safe_ckeywordv(fields[j].name);
            janet_table_put(t, k, jv);
        }
        janet_array_push(a, janet_wrap_table(t));
    }

    return janet_wrap_array(a);
}

static Janet decode_binary(MYSQL_BIND *bind, MYSQL_FIELD *field) {
    if (*bind->is_null) {
        return janet_wrap_nil();
    }

    int t = bind->buffer_type;
    char *v = bind->buffer;
    int l = *bind->length;

    Janet jv;

    switch (t) {
        case MYSQL_TYPE_NULL:
            jv = janet_wrap_nil();
            break;
        case MYSQL_TYPE_TINY:
            if (field->length == 1) {
                jv = janet_wrap_boolean(*((char *)v));
            } else {
                jv = janet_wrap_number(*((char *)v));
            }
            break;

        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_YEAR:
            jv = janet_wrap_number(*((short int *)v));
            break;

        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
            jv = janet_wrap_number(*((int *)v));
            break;

        case MYSQL_TYPE_FLOAT:
            jv = janet_wrap_number(*((float *)v));
            break;

        case MYSQL_TYPE_DOUBLE:
            jv = janet_wrap_number(*((double *)v));
            break;

        case MYSQL_TYPE_LONGLONG:
            jv = janet_wrap_number(*((long long int *)v));
            break;

        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP2: {
            MYSQL_TIME t = *(MYSQL_TIME *)v;
            switch (t.time_type) {
                case MYSQL_TIMESTAMP_DATE: {
                    JanetKV *st = janet_struct_begin(3);
                    janet_struct_put(st, janet_ckeywordv("day"), janet_wrap_number(t.day));
                    janet_struct_put(st, janet_ckeywordv("month"), janet_wrap_number(t.month));
                    janet_struct_put(st, janet_ckeywordv("year"), janet_wrap_number(t.year));

                    jv = janet_wrap_struct(janet_struct_end(st));
                    break;
                }

                //Value is in UTC for `TIMESTAMP` type.
                //Value is in local time zone for `DATETIME` type.
                case MYSQL_TIMESTAMP_DATETIME: {
                    int count;
                    if (field->type != MYSQL_TYPE_TIMESTAMP && field->type != MYSQL_TYPE_TIMESTAMP2) {
                        count = 8;
                    } else {
                        count = 7;
                    }
                    JanetKV *st = janet_struct_begin(count);
                    janet_struct_put(st, janet_ckeywordv("microseconds"), janet_wrap_number(t.second_part));
                    janet_struct_put(st, janet_ckeywordv("seconds"), janet_wrap_number(t.second));
                    janet_struct_put(st, janet_ckeywordv("minutes"), janet_wrap_number(t.minute));
                    janet_struct_put(st, janet_ckeywordv("hours"), janet_wrap_number(t.hour));
                    janet_struct_put(st, janet_ckeywordv("day"), janet_wrap_number(t.day));
                    janet_struct_put(st, janet_ckeywordv("month"), janet_wrap_number(t.month));
                    janet_struct_put(st, janet_ckeywordv("year"), janet_wrap_number(t.year));
                    if (field->type != MYSQL_TYPE_TIMESTAMP && field->type != MYSQL_TYPE_TIMESTAMP2) {
                        janet_struct_put(st, janet_ckeywordv("tz"), janet_wrap_number(t.time_zone_displacement));
                    }
                    jv = janet_wrap_struct(janet_struct_end(st));
                    break;
                }

                /// Stores hour, minute, second and microsecond.
                case MYSQL_TIMESTAMP_TIME: {
                    JanetKV *st = janet_struct_begin(3);
                    janet_struct_put(st, janet_ckeywordv("seconds"), janet_wrap_number(t.second));
                    janet_struct_put(st, janet_ckeywordv("minutes"), janet_wrap_number(t.minute));
                    janet_struct_put(st, janet_ckeywordv("hours"), janet_wrap_number(t.hour));
                    jv = janet_wrap_struct(janet_struct_end(st));
                    break;
                }

                case MYSQL_TIMESTAMP_NONE:
                case MYSQL_TIMESTAMP_ERROR:
                case MYSQL_TIMESTAMP_DATETIME_TZ:
                    janet_panicf("unexpected time type %d\n", t.time_type);
                    break;
            }
        }
        break;

        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            jv = janet_wrap_string(janet_string((uint8_t *)v, l));
            break;

        default:
            //MYSQL_TYPE_ENUM = 247,
            //MYSQL_TYPE_SET = 248,
            //MYSQL_TYPE_GEOMETRY = 255
            janet_panicf("unexpected mysql type %d\n", t);
    }
    return jv;
}


static Janet rows_binary_unpack(jmy_rows_t *rows) {
    int num_fields = rows->num_fields;
    MYSQL_FIELD *fields = mysql_fetch_fields(rows->r);

    jmy_query_bind_t binds = allocate_binds(num_fields, fields);
    if (mysql_stmt_bind_result(rows->statement, binds.binds)) {
        stmt_panic(rows->statement, "mysql_stmt_bind_result");
    }

    JanetArray *a = janet_array(0);
    while (1) {
        int status = mysql_stmt_fetch(rows->statement);
        if (status == 1 || status == MYSQL_NO_DATA) {
            break;
        }
        if (status == MYSQL_DATA_TRUNCATED) {
            janet_panicf("mysql_stmt_fetch failed: MYSQL_DATA_TRUNCATED\n");
        }

        JanetTable *t = janet_table(num_fields);
        for (int j = 0; j < num_fields; ++j) {
            if (*binds.binds[j].error) {
                janet_panicf("unexpected error in field %d", j);
            }
            Janet jv = decode_binary(&binds.binds[j], &fields[j]);
            Janet k = safe_ckeywordv(fields[j].name);
            janet_table_put(t, k, jv);
        }

        janet_array_push(a, janet_wrap_table(t));
    }

    mysql_free_result(rows->r);
    rows->r = NULL;

    query_bind_free(binds);

    return janet_wrap_array(a);
}

static Janet rows_unpack(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_rows_t *rows = (jmy_rows_t *)janet_getabstract(argv, 0, &rows_type);
    __ensure_rows_ok(rows);

    if (rows->statement == NULL) {
        return rows_text_unpack(rows);
    } else {
        return rows_binary_unpack(rows);
    }
}

static int count_subs(const char *q) {
    int count = 0;
    while (*q != '\0') {
        if (*q == '?') {
            count++;
        }
        q++;
    }
    return count;
}

static char *interpolate_params(MYSQL *conn, const char *q, size_t len, int32_t argc, Janet *argv) {
    int n_subs = count_subs(q);
    char **pvals = janet_smalloc(n_subs * sizeof(char *));
    int *plengths = janet_smalloc(n_subs * sizeof(int));
    if (n_subs != argc) {
        janet_panicf("query: wrong arity %d expected got %d\n", n_subs, argc);
    }

    for (int i = 0; i < argc; i++) {
        Janet j = argv[i];

        switch (janet_type(j)) {
            case JANET_NIL: {
                const char *null = "NULL";
                size_t len = strlen(null);
                pvals[i] = janet_smalloc(len + 1);
                strcpy(pvals[i], null);
                plengths[i] = len;
                break;
            }

            case JANET_BOOLEAN: {
                pvals[i] = janet_smalloc(2);
                plengths[i] = 1;
                if (janet_unwrap_boolean(j)) {
                    pvals[i][0] = '1';
                } else {
                    pvals[i][0] = '0';
                }
                pvals[i][1] = '\0';
                break;
            }

            case JANET_BUFFER: {
                JanetBuffer *b = janet_unwrap_buffer(j);
                pvals[i] = janet_smalloc(b->count);
                memcpy(pvals[i], b->data, b->count);
                plengths[i] = b->count;
                break;
            }

            case JANET_KEYWORD:
            case JANET_STRING: {
                const char *s = (char *)janet_unwrap_string(j);
                size_t l = janet_string_length(s);
                pvals[i] = janet_smalloc(l * 2 + 2 + 1);
                pvals[i][0] = '\'';
                unsigned long ol = mysql_real_escape_string_quote(conn, pvals[i] + 1, s, l, '\'');
                pvals[i][1 + ol] = '\'';
                plengths[i] = ol + 2;
                break;
            }

            case JANET_NUMBER: {
                double d = janet_unwrap_number(j);
                /* The exact range could be increased to ~52 bits afaik, for now 32 bits
                * only. */
                const char *fmt = (d == floor(d) && d <= ((double)INT32_MAX) &&
                                   d >= ((double)INT32_MIN))
                                  ? "%.0f"
                                  : "%g";
                size_t l = snprintf(NULL, 0, fmt, d);
                pvals[i] = janet_smalloc(l + 1);
                snprintf(pvals[i], l + 1, fmt, d);
                plengths[i] = l;
                break;
            }

            case JANET_ABSTRACT: {
                JanetIntType intt = janet_is_int(j);
                if (intt == JANET_INT_S64) {
                    int64_t v = janet_unwrap_s64(j);
                    size_t l = snprintf(NULL, 0, "%" PRId64, v);
                    pvals[i] = janet_smalloc(l + 1);
                    snprintf(pvals[i], l + 1, "%" PRId64, v);
                    plengths[i] = l;
                    break;
                } else if (intt == JANET_INT_U64) {
                    uint64_t v = janet_unwrap_u64(j);
                    size_t l = snprintf(NULL, 0, "%" PRIu64, v);
                    pvals[i] = janet_smalloc(l + 1);
                    snprintf(pvals[i], l + 1, "%" PRIu64, v);
                    plengths[i] = l;
                    break;
                } else {
                    /* fall-thru */
                }
            }
            /* fall-thru */

            default:
                janet_panicf("cannot encode janet type %d", janet_type(j));
        }
    }

    int total = 0;
    for (int i = 0; i < argc; i++) {
        total += plengths[i];
    }
    char *query = janet_smalloc(len + total + 1);
    char *ps = query;
    const char *pq = q;
    int pi = 0;
    while (*pq != '\0') {
        if (*pq == '?') {
            memcpy(ps, pvals[pi], plengths[pi]);
            ps += plengths[pi];
            pi++;
        } else {
            *ps++ = *pq;
        }
        pq++;
    }

    *ps = '\0';

    /* Free in reverse order for sfree's sake */
    for (int i = argc - 1; i > 0; i--) {
        janet_sfree(pvals[i]);
    }
    janet_sfree(pvals);
    janet_sfree(plengths);

    return query;
}

static Janet text_exec(jmy_context_t *ctx, int32_t argc, Janet *argv) {
    const char *q = janet_getcstring(argv, 0);
    int len = strlen(q);

    argc -= 1;
    argv += 1;

    char *query = interpolate_params(ctx->conn, q, len, argc, argv);
    if (mysql_real_query(ctx->conn, query, strlen(query))) {
        janet_panicf("mysql_real_query failed: %s\n", mysql_error(ctx->conn));
    }

    janet_sfree(query);

    /* the column count is > 0 if there is a result set */
    /* 0 if the result is only the final status packet */
    int num_fields = mysql_field_count(ctx->conn);
    if (num_fields > 0) {
        janet_panicf("mysql_field_count unexpected returned 0\n");
    }

    jmy_result_t *result = (jmy_result_t *)janet_abstract(&result_type, sizeof(jmy_result_t));
    result->affected_rows = mysql_affected_rows(ctx->conn);
    result->insert_id = mysql_insert_id(ctx->conn);
    return janet_wrap_abstract(result);
}

static Janet text_select(jmy_context_t *ctx, int32_t argc, Janet *argv) {
    const char *q = janet_getcstring(argv, 1);
    int len = strlen(q);

    argc -= 2;
    argv += 2;

    char *query = interpolate_params(ctx->conn, q, len, argc, argv);
    if (mysql_real_query(ctx->conn, query, strlen(query))) {
        janet_panicf("mysql_real_query failed: %s\n", mysql_error(ctx->conn));
    }
    janet_sfree(query);

    int num_fields = mysql_field_count(ctx->conn);
    if (num_fields == 0) {
        janet_panicf("mysql_field_count unexpected returned 0\n");
    }

    MYSQL_RES *r = mysql_store_result(ctx->conn);
    if (r == NULL) {
        janet_panicf("mysql_store_result failed: %s\n", mysql_error(ctx->conn));
    }

    jmy_rows_t *rows = (jmy_rows_t *)janet_abstract(&rows_type, sizeof(jmy_rows_t));
    rows->num_fields = num_fields;
    rows->r = r;
    rows->statement = NULL;

    return janet_wrap_abstract(rows);
}

static Janet context_exec(int32_t argc, Janet *argv) {
    if (argc < 2) {
        janet_panic("expected at least a pq context and a query string");
    }
    if (argc > 10000000) {
        janet_panic("too many arguments");
    }

    if (janet_checkabstract(argv[0], &context_type)) {
        jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
        __ensure_ctx_ok(ctx);
        argc -= 1;
        argv += 1;
        return text_exec(ctx, argc, argv);
    }

    if (janet_checkabstract(argv[0], &statement_type)) {
        jmy_statement_t *stmt = (jmy_statement_t *)janet_getabstract(argv, 0, &statement_type);
        __ensure_stmt_ok(stmt);
        argc -= 1;
        argv += 1;
        return stmt_exec(stmt, argc, argv);
    }
    janet_panicf("error: bad slot #0, expected mysql/connection or mysql/stmt, got %v", argv[0]);
    return janet_wrap_nil();
}

static Janet context_select(int32_t argc, Janet *argv) {
    if (argc < 2) {
        janet_panic("expected at least a pq context and a query string");
    }
    if (argc > 10000000) {
        janet_panic("too many arguments");
    }

    if (janet_checkabstract(argv[0], &context_type)) {
        jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
        __ensure_ctx_ok(ctx);
        return text_select(ctx, argc, argv);
    }
    if (janet_checkabstract(argv[0], &statement_type)) {
        jmy_statement_t *stmt = (jmy_statement_t *)janet_getabstract(argv, 0, &statement_type);
        __ensure_stmt_ok(stmt);
        return stmt_select(stmt, argc, argv);
    }
    janet_panicf("error: bad slot #0, expected mysql/connection or mysql/stmt, got %v", argv[0]);
    return janet_wrap_nil();
}


static Janet context_status(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);
    return janet_wrap_integer(mysql_ping(ctx->conn));
}

static Janet context_close(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);
    context_close_i(ctx);
    return janet_wrap_nil();
}

static Janet context_begin(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);

    const char *start = "start transaction";
    if (mysql_real_query(ctx->conn, start, strlen(start))) {
        janet_panicf("mysql_real_query failed %s\n", mysql_error(ctx->conn));
    }
    ctx->in_transaction = true;
    return janet_wrap_nil();
}

static Janet context_commit(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);

    if (mysql_commit(ctx->conn)) {
        janet_panicf("mysql_commit failed %s\n", mysql_error(ctx->conn));
    }
    ctx->in_transaction = false;
    return janet_wrap_nil();
}

static Janet context_rollback(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);

    if (mysql_rollback(ctx->conn)) {
        janet_panicf("mysql_rollback failed %s\n", mysql_error(ctx->conn));
    }
    ctx->in_transaction = false;
    return janet_wrap_nil();
}

static Janet context_in_transaction(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);
    return janet_wrap_boolean(ctx->in_transaction);
}

static Janet context_autocommit(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    jmy_context_t *ctx = (jmy_context_t *)janet_getabstract(argv, 0, &context_type);
    __ensure_ctx_ok(ctx);
    bool ok = janet_getboolean(argv, 1);
    if (mysql_autocommit(ctx->conn, ok)) {
        janet_panicf("mysql_commit failed %s\n", mysql_error(ctx->conn));
    }
    return janet_wrap_nil();
}

#define upstream_doc "See libpq documentation at https://www.postgresql.org."

static const JanetReg cfuns[] = {
    {
        "connect", context_connect,
        "(mysql/connect url)\n\n"
        "Connect to a postgres server or raise an error."
    },

    {
        "select-db", context_select_db,
        "(mysql/select-db db)\n\n"
        "Select a database."
    },

    {
        "close", context_close,
        "(mysql/close ctx)\n\n"
        "Close a mysql context."
    },
    {"status", context_status, upstream_doc},
    {
        "autocommit", context_autocommit,
        "(mysql/autocommit conn mode)\n\n"
        "Enable or disable autocommit."
    },


    // exec and select.
    {"exec", context_exec, "See mysql/exec"},
    {"select", context_select, "See mysql/select"},

    // statements.
    {"prepare", context_prepare, "See mysql/exec"},
    {"stmt-close", stmt_close, "See mysql/exec"},

    // transactions.
    {"begin", context_begin, upstream_doc},
    {"commit", context_commit, upstream_doc},
    {"rollback", context_rollback, upstream_doc},
    {"in-transaction", context_in_transaction, upstream_doc},

    // result functions
    {"result-insert-id", result_insert_id, upstream_doc},
    {"result-affected-rows", result_affected_rows, upstream_doc},

    // rows functions
    {"rows-columns", rows_columns, upstream_doc},
    {"rows-column-types", rows_column_types, upstream_doc},
    {"rows-unpack", rows_unpack, upstream_doc},

    {NULL, NULL, NULL}
};

JANET_MODULE_ENTRY(JanetTable *env) {

    janet_cfuns(env, "pq", cfuns);
    janet_register_abstract_type(&context_type);
    janet_register_abstract_type(&rows_type);
}
