#include <iostream>
#include "xdbc_XClient.h"
#include <xclient.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <fstream>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

xdbc::RuntimeEnv* globalRuntimeEnv = nullptr;
/*
 * Class:      xdbc_Library_00024
 * Method:     load
 * Signature:  (Ljava/lang/String;)I
 */
std::vector <xdbc::SchemaAttribute> createSchemaFromConfig(const std::string &configFile) {
     std::ifstream file(configFile);
     if (!file.is_open()) {
         std::cout << "failed to open schema" << configFile << std::endl;

     }
     nlohmann::json schemaJson;
     file >> schemaJson;

     std::vector <xdbc::SchemaAttribute> schema;
     for (const auto &item: schemaJson) {
         schema.emplace_back(xdbc::SchemaAttribute{
                 item["name"],
                 item["type"],
                 item["size"]
         });
     }
     return schema;
 }
std::string readJsonFileIntoString(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
         std::cout << "failed to open schema" << filePath << std::endl;
         return "";
    }

    std::stringstream buffer;
    buffer << file.rdbuf();

    return buffer.str();
}
JNIEXPORT jlong JNICALL Java_xdbc_XClient_initialize
  (JNIEnv *env, jobject clazz, jstring hostname, jstring name, jstring tableName, jlong transfer_id, jint iformat,
   jint buffer_size, jint bpool_size, jint rcv_par, jint decomp_par, jint write_par){

    auto start_profiling = std::chrono::steady_clock::now();
    auto console = spdlog::stdout_color_mt("SparkXCLIENT");
    const char* host_chars = env->GetStringUTFChars(hostname,0);
    const char* name_chars = env->GetStringUTFChars(name,0);
    const char* tableName_chars = env->GetStringUTFChars(tableName,0);
    xdbc::RuntimeEnv* xdbcEnv = new xdbc::RuntimeEnv();
    globalRuntimeEnv = xdbcEnv;
    xdbcEnv->env_name = name_chars;
    xdbcEnv->table = tableName_chars;
    xdbcEnv->iformat = iformat;
    xdbcEnv->buffer_size = buffer_size;
    xdbcEnv->buffers_in_bufferpool = bpool_size / xdbcEnv->buffer_size;
    xdbcEnv->sleep_time = std::chrono::milliseconds(1);
    xdbcEnv->rcv_parallelism = rcv_par;
    xdbcEnv->decomp_parallelism = decomp_par;
    xdbcEnv->write_parallelism = write_par;
    xdbcEnv->transfer_id = transfer_id;

    xdbcEnv->server_host = host_chars;
    xdbcEnv->server_port = "1234";

    //create schema
    std::vector <xdbc::SchemaAttribute> schema;

    std::string schemaFile = "/xdbc-client/tests/schemas/" + xdbcEnv->table + ".json";

    schema = createSchemaFromConfig(schemaFile);
    xdbcEnv->schemaJSON = readJsonFileIntoString(schemaFile);
    xdbcEnv->schema = schema;

    xdbcEnv->tuple_size = std::accumulate(xdbcEnv->schema.begin(), xdbcEnv->schema.end(), 0,
                                     [](int acc, const xdbc::SchemaAttribute &attr) {
                                         return acc + attr.size;
                                     });

    xdbcEnv->tuples_per_buffer = xdbcEnv->buffer_size * 1024 / xdbcEnv->tuple_size;

    xdbc::XClient* c = new xdbc::XClient(*xdbcEnv);

    env->ReleaseStringUTFChars(name, name_chars);
    env->ReleaseStringUTFChars(tableName, tableName_chars);
    env->ReleaseStringUTFChars(hostname, host_chars);

    spdlog::get("SparkXCLIENT")->info("Created XClient: {}", c->get_name());
    return reinterpret_cast<jlong>(c);
}
JNIEXPORT jlong JNICALL Java_xdbc_XClient_startReceiving0
  (JNIEnv *env, jobject clazz, jlong pointer, jstring tableName) {
    spdlog::get("SparkXCLIENT")->info("Entered receive with pointer {}", pointer );
    const char* name_chars = env->GetStringUTFChars(tableName,0);
    xdbc::XClient* c = (xdbc::XClient*) pointer;

    spdlog::get("SparkXCLIENT")->info("Entered receive of {}", c->get_name());
    c->startReceiving(name_chars);
    env->ReleaseStringUTFChars(tableName, name_chars);

    globalRuntimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), 0, "write", "start"});

    return 1;
}
/*
 * Class:      xdbc_XClient
 * Method:     hasNext
 * Signature:  (J)I
 */
JNIEXPORT jint JNICALL Java_xdbc_XClient_hasNext0
  (JNIEnv *env, jobject clazz, jlong pointer)
  {
    xdbc::XClient* c = (xdbc::XClient*) pointer;
    //spdlog::get("SparkXCLIENT")->info("Entered hasNext of {}", c->get_name());
    if(c->hasNext(0))
            return 1;

        return 0;
  }

/*
 * Class:      xdbc_XClient
 * Method:     getBuffer
 * Signature:  (J)J
 */
JNIEXPORT jint JNICALL Java_xdbc_XClient_getBuffer0
  (JNIEnv* env, jobject clazz, jlong pointer, jobject resBuf, jint tuple_size)
{
    //TODO: we should get buffer size from xdbcEnv instead of the parameter
    // Get direct buffer address for resBuf
    jbyte* res = (jbyte*) env->GetDirectBufferAddress(resBuf);

    // Get XClient instance
    xdbc::XClient* c = (xdbc::XClient*) pointer;

    //spdlog::get("SparkXCLIENT")->info("Entered getBuffer of {}", c->get_name());

    // Fetch buffer and metadata (buffWithId) from XClient
    xdbc::buffWithId curBuffWithId = c->getBuffer(0);
    if (curBuffWithId.id >= 0) {

        //spdlog::get("SparkXCLIENT")->info("getBuffer received buff Id: {} with # tuples:{} ", curBuffWithId.id, curBuffWithId.totalTuples);

        std::memcpy(res, curBuffWithId.buff, curBuffWithId.totalTuples * tuple_size);

        c->markBufferAsRead(curBuffWithId.id);
        globalRuntimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), 0, "write", "push"});

        jclass cls = env->GetObjectClass(resBuf);
        jmethodID mid = env->GetMethodID(cls, "limit", "(I)Ljava/nio/Buffer;");
        env->CallObjectMethod(resBuf, mid, curBuffWithId.totalTuples * tuple_size);
    }else {
        spdlog::get("SparkXCLIENT")->info("Found buffer with id {}", curBuffWithId.id);
    }


    return curBuffWithId.id;
}

/*
 * Class:      xdbc_XClient
 * Method:     markBufferAsRead
 * Signature:  (JI)I
 */
 //TODO: can be removed, not used on the Scala side
JNIEXPORT jint JNICALL Java_xdbc_XClient_markBufferAsRead0
  (JNIEnv *env, jobject clazz, jlong pointer, jint bufferId)
  {
    xdbc::XClient* c = (xdbc::XClient*) pointer;
    spdlog::get("SparkXCLIENT")->info("Entered marBufferAsRead of {}", c->get_name());

    c->markBufferAsRead(bufferId);
    return 1;
  }

/*
 * Class:      xdbc_XClient
 * Method:     finalize
 * Signature:  (J)I
 */
JNIEXPORT jint JNICALL Java_xdbc_XClient_finalize0
  (JNIEnv *env, jobject clazz, jlong pointer)
{
    globalRuntimeEnv->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), 0, "write", "end"});
    xdbc::XClient* c = (xdbc::XClient*) pointer;
    c->finalize();
    spdlog::get("SparkXCLIENT")->info("Called finalize on {}", c->get_name());
    return 1;
}