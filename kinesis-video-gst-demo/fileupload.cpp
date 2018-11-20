#include <gst/gst.h>
#include <gst/app/gstappsink.h>
#include <glib/gstrfuncs.h>
#include <stdlib.h>
#include <string.h>
#include <chrono>
#include <Logger.h>
#include <vector>
#include <thread>
#include <mutex>
#include <iostream>

#include "memorystats.h"
#include "queue.h"
#include "KinesisVideoProducer.h"
#include "../kinesis-video-gstreamer-plugin/plugin-src/KvsSinkIotCertCredentialProvider.h"

using namespace std;
using namespace com::amazonaws::kinesis::video;
using namespace log4cplus;

#ifdef __cplusplus
extern "C" {
#endif

int gstreamer_init(int, char **);
int gstreamer_start(char *, char *);

#ifdef __cplusplus
}
#endif

LOGGER_TAG("com.amazonaws.kinesis.video.gstreamer");

#define ACCESS_KEY_ENV_VAR "AWS_ACCESS_KEY_ID"
#define DEFAULT_REGION_ENV_VAR "AWS_DEFAULT_REGION"
#define DEFAULT_STREAM_NAME "STREAM_NAME"
#define SECRET_KEY_ENV_VAR "AWS_SECRET_ACCESS_KEY"
#define SESSION_TOKEN_ENV_VAR "AWS_SESSION_TOKEN"

//Test setup
#define CLIP_QUEUE_SIZE_MAX_THRESHOLD 2

//Kinesis Video Stream definitions
#define AVG_BANDWIDTH_BPS (4 * 1024 * 1024)
#define BUFFER_DURATION_IN_SECS 120
#define CONNECTION_STALENESS_IN_SECS 30
#define DEFAULT_FRAME_DATA_SIZE_BYTE (1024*1024)
#define REPLAY_DURATION_IN_SECS 40
#define STORAGE_SIZE (3 * 1024 * 1024)

static mutex custom_data_mtx;

namespace com { namespace amazonaws { namespace kinesis { namespace video {
typedef struct _CustomData {
  GstElement *pipeline, *source, *filter, *appsink, *h264parse, *tsdemux;
  GstBus *bus;
  GMainLoop *main_loop;
  unique_ptr<KinesisVideoProducer> kinesis_video_producer;
  shared_ptr<KinesisVideoStream> kinesis_video_stream;
  char stream_name[MAX_STREAM_NAME_LEN+1];
  volatile bool connection_error;
  volatile bool stream_in_progress;
  uint8_t *frame_data;
  size_t current_frame_data_size;

} CustomData;


auto getStreamDefinitionPtr(const CustomData *data);

CustomData data;
char gclip[MAX_PATH_LEN+1] = "";
char glevelclip[MAX_PATH_LEN+1] = "";
const gchar *audiopad = "audio";
const gchar *videopad = "video";
ClipQueue clip_queue;

auto getStreamDefinitionPtr(const CustomData *data) {
  auto stream_definition = make_unique<StreamDefinition>(data->stream_name,
                                                         hours(2),
                                                         nullptr, //no tags
                                                         "",
                                                         STREAMING_TYPE_REALTIME,
                                                         "video/h264",
                                                         milliseconds::zero(),
                                                         seconds(2),
                                                         milliseconds(1),
                                                         true,
                                                         true,
                                                         true,
                                                         true,
                                                         true,
                                                         true,
                                                         NAL_ADAPTATION_FLAG_NONE,
                                                         10,
                                                         AVG_BANDWIDTH_BPS,
                                                         seconds(BUFFER_DURATION_IN_SECS),
                                                         seconds(REPLAY_DURATION_IN_SECS),
                                                         seconds(CONNECTION_STALENESS_IN_SECS),
                                                         "V_MPEG4/ISO/AVC",
                                                         "kinesis_video",
                                                         nullptr, // cpd derived from media pipeline
                                                         0);
  return stream_definition;
}

class SampleClientCallbackProvider : public ClientCallbackProvider {
 public:
  UINT64 getCallbackCustomData() override {
    return reinterpret_cast<UINT64> (this);
  }

  StorageOverflowPressureFunc getStorageOverflowPressureCallback() override {
    return storageOverflowPressure;
  }

  static STATUS storageOverflowPressure(UINT64 custom_handle, UINT64 remaining_bytes);
};

class SampleStreamCallbackProvider : public StreamCallbackProvider {
  CustomData *data;
 public:
  SampleStreamCallbackProvider(CustomData *data) : data(data) {}

  UINT64 getCallbackCustomData() override {
    return reinterpret_cast<UINT64> (data);
  }

  StreamConnectionStaleFunc getStreamConnectionStaleCallback() override {
    return streamConnectionStaleHandler;
  };

  StreamErrorReportFunc getStreamErrorReportCallback() override {
    return streamErrorReportHandler;
  };

  DroppedFrameReportFunc getDroppedFrameReportCallback() override {
    return droppedFrameReportHandler;
  };

  FragmentAckReceivedFunc getFragmentAckReceivedCallback() override {
    return FragmentAckReceivedHandler;
  };

 private:
  static STATUS
  streamConnectionStaleHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                               UINT64 last_buffering_ack);

  static STATUS
  streamErrorReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle, UINT64 errored_timecode,
                           STATUS status_code);

  static STATUS
  droppedFrameReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                            UINT64 dropped_frame_timecode);

  static STATUS
  FragmentAckReceivedHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                             PFragmentAck pAckReceived);
};

class SampleCredentialProvider : public StaticCredentialProvider {
  // Test rotation period is 40 second for the grace period.
  const std::chrono::duration<uint64_t> ROTATION_PERIOD = std::chrono::seconds(2400);
 public:
  SampleCredentialProvider(const Credentials &credentials) :
      StaticCredentialProvider(credentials) {}

  void updateCredentials(Credentials &credentials) override {
    // Copy the stored creds forward
    credentials = credentials_;

    // Update only the expiration
    auto now_time = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch());
    auto expiration_seconds = now_time + ROTATION_PERIOD;
    credentials.setExpiration(std::chrono::seconds(expiration_seconds.count()));
    LOG_INFO("New credentials expiration is " << credentials.getExpiration().count());
  }
};

class SampleDeviceInfoProvider : public DefaultDeviceInfoProvider {
 public:
  device_info_t getDeviceInfo() override {
    auto device_info = DefaultDeviceInfoProvider::getDeviceInfo();
    device_info.storageInfo.storageSize = STORAGE_SIZE;
    return device_info;
  }
};

STATUS
SampleClientCallbackProvider::storageOverflowPressure(UINT64 custom_handle, UINT64 remaining_bytes) {
  UNUSED_PARAM(custom_handle);
  LOG_WARN("Reporting storage overflow. Bytes remaining " << remaining_bytes);
  return STATUS_SUCCESS;
}

STATUS SampleStreamCallbackProvider::streamConnectionStaleHandler(UINT64 custom_data,
                                                                  STREAM_HANDLE stream_handle,
                                                                  UINT64 last_buffering_ack) {
  LOG_WARN("Reporting stream stale. Last ACK received " << last_buffering_ack);
  return STATUS_SUCCESS;
}

STATUS
SampleStreamCallbackProvider::streamErrorReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                                                       UINT64 errored_timecode, STATUS status_code) {
  LOG_ERROR("Reporting stream error. Errored timecode: " << errored_timecode << " Status: "
                                                         << status_code);
  auto customDataObj = reinterpret_cast<CustomData *>(custom_data);
  if (status_code == static_cast<UINT32>(STATUS_DESCRIBE_STREAM_CALL_FAILED) &&
      customDataObj->kinesis_video_stream != NULL) {
    {
      std::lock_guard<std::mutex> lk(custom_data_mtx);
      //given the requirement to drop/flush old buffers mark for restarting
      // the pipeline when requested for sending the next incoming clip
      customDataObj->connection_error = true;
      customDataObj->stream_in_progress = false;
    }
  }
  return STATUS_SUCCESS;
}

STATUS
SampleStreamCallbackProvider::droppedFrameReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                                                        UINT64 dropped_frame_timecode) {
  LOG_DEBUG("Reporting dropped frame. Frame timecode " << dropped_frame_timecode);
  auto customDataObj = reinterpret_cast<CustomData *>(custom_data);

  if( customDataObj->kinesis_video_stream!=NULL) {

    std::lock_guard<std::mutex> lk(custom_data_mtx);
    //given the requirement to drop/flush old buffers mark for restarting
    // the pipeline when requested for sending the next incoming clip
    customDataObj->connection_error = true;
    customDataObj->stream_in_progress = false;

  }
  return STATUS_SUCCESS;
}

STATUS
SampleStreamCallbackProvider::FragmentAckReceivedHandler(UINT64 custom_data,
                                                         STREAM_HANDLE stream_handle,
                                                         PFragmentAck pFragmentAck) {

  if (pFragmentAck->ackType == FRAGMENT_ACK_TYPE_PERSISTED) {
    LOG_INFO("Reporting fragment ACK received. Fragment timecode " << pFragmentAck->timestamp);
  }
  return STATUS_SUCCESS;
}

}  // namespace video
}  // namespace kinesis
}  // namespace amazonaws
}  // namespace com;

unique_ptr<Credentials> credentials_;


void create_kinesis_video_frame(Frame *frame, FRAME_FLAGS flags, void *data, size_t len) {
  nanoseconds current_time = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()
  );
  frame->flags = flags;
  frame->decodingTs = static_cast<UINT64>(current_time.count()) / DEFAULT_TIME_UNIT_IN_NANOS;
  frame->presentationTs = static_cast<UINT64>(current_time.count()) / DEFAULT_TIME_UNIT_IN_NANOS;
  frame->duration = 20 * HUNDREDS_OF_NANOS_IN_A_MILLISECOND;
  frame->size = static_cast<UINT32>(len);
  frame->frameData = reinterpret_cast<PBYTE>(data);
}

bool put_frame(shared_ptr<KinesisVideoStream> kinesis_video_stream, void *data, size_t len, FRAME_FLAGS flags) {
  Frame frame;
  create_kinesis_video_frame(&frame, flags, data, len);
  return kinesis_video_stream->putFrame(frame);
}

static GstFlowReturn on_new_sample(GstElement *sink, CustomData *data) {
  GstSample *sample = gst_app_sink_pull_sample(GST_APP_SINK (sink));
  GstCaps *gstcaps = gst_sample_get_caps(sample);
  GstStructure *gststructforcaps = gst_caps_get_structure(gstcaps, 0);

  if (!data->stream_in_progress) {
    data->stream_in_progress = true;
    const GValue *gstStreamFormat = gst_structure_get_value(gststructforcaps, "codec_data");
    gchar *cpd = gst_value_serialize(gstStreamFormat);
    data->kinesis_video_stream->start(std::string(cpd));
    LOG_INFO("adding codec private data");
    g_free(cpd);
  }

  bool connection_error;
  {
    std::lock_guard<std::mutex> lk(custom_data_mtx);
    connection_error = data->connection_error;
  }
  // dropping all incoming frames as we are restarting streams. A better solution in
  // the future is to direct them to file storage.
  if (!connection_error) {
    GstBuffer *buffer = gst_sample_get_buffer(sample);
    size_t buffer_size = gst_buffer_get_size(buffer);

    if (buffer_size > data->current_frame_data_size) {
      delete [] data->frame_data;
      data->current_frame_data_size= buffer_size * 2;
      data->frame_data = new uint8_t[data->current_frame_data_size];
    }

    gst_buffer_extract(buffer, 0, data->frame_data, buffer_size);

    bool delta = GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT);
    FRAME_FLAGS kinesis_video_flags;

    if (!delta) {
      kinesis_video_flags = FRAME_FLAG_KEY_FRAME;
    } else {
      kinesis_video_flags = FRAME_FLAG_NONE;
    }

    if (false == put_frame(data->kinesis_video_stream, data->frame_data, buffer_size, kinesis_video_flags)) {
      LOG_WARN("Failed to put_frame");
    }
  } else {
    gst_sample_unref(sample);
    return GST_FLOW_EOS;
  }

  gst_sample_unref(sample);
  return GST_FLOW_OK;
}

/* This function is called when an error message is posted on the bus */
static void error_cb(GstBus *bus, GstMessage *msg, CustomData *data) {
  GError *err;
  gchar *debug_info;

  /* Print error details */
  gst_message_parse_error(msg, &err, &debug_info);
  g_printerr("Error received from element %s: %s\n", GST_OBJECT_NAME (msg->src), err->message);
  g_printerr("Debugging information: %s\n", debug_info ? debug_info : "none");
  g_clear_error(&err);
  g_free(debug_info);

  g_main_loop_quit(data->main_loop);
}

static void cb_ts_pad_created(GstElement *element, GstPad *pad, CustomData *data) {
  gchar *pad_name = gst_pad_get_name(pad);

  g_print("New TS source pad found: %s\n", pad_name);
  if (g_str_has_prefix(pad_name, videopad)) {
    if (gst_element_link_pads(data->tsdemux, pad_name, data->h264parse, "sink")) {
      g_print("Video source pad linked successfully.\n");
    } else {
      g_printerr("Video source pad link failed\n");
    }
    g_free(pad_name);
  } else if (g_str_has_prefix(pad_name, audiopad)) {
    g_print("Audio pad has been detected\n");
    g_free(pad_name);
  }

}

static void cb_message(GstBus *bus, GstMessage *msg, CustomData *data) {
  FRAME_FLAGS kinesis_video_flags;
  kinesis_video_flags = FRAME_FLAG_KEY_FRAME;
  uint8_t dummy_frame;
  switch (GST_MESSAGE_TYPE (msg)) {
    case GST_MESSAGE_ERROR: {
      GError *err;
      gchar *debug;

      gst_message_parse_error(msg, &err, &debug);
      g_print("Error: %s\n", err->message);
      g_error_free(err);
      g_free(debug);

      gst_element_set_state(data->pipeline, GST_STATE_NULL);
      g_main_loop_quit(data->main_loop);
      break;
    }
    case GST_MESSAGE_EOS: {
      /* end-of-stream */
      g_print("message : GST_MESSAGE_EOS\n");
      if (data->kinesis_video_stream!=NULL) {
        //TODO: remove this once we have EoFr
        if (!put_frame(data->kinesis_video_stream, &dummy_frame, 1, kinesis_video_flags)) {
          g_printerr("Error in streaming for the clip !\n");
        } else {
          g_print("Clip complete %s\n", gclip);
          clip_queue.remove();
        }
      }

      {
        std::lock_guard<std::mutex> lk(custom_data_mtx);
        data->stream_in_progress = false;
      }


      g_main_loop_quit(data->main_loop);

      if (strlen(gclip)!=0) {
        g_print("Deleting clips: %s : %s clip queue size=%d\n", gclip, glevelclip, clip_queue.size());
        // Note: Commented for local test run with the same clip every time
        // when running from actual camera device
        // within the daemon, uncomment so that the files are cleared up.
        //unlink(gclip);
        //unlink(glevelclip);
      }
      break;
    }
    case GST_MESSAGE_CLOCK_LOST: {
      /* Get a new clock */
      g_print("message : GST_MESSAGE_CLOCK_LOST\n");
      break;
    }
    default:
      /* Unhandled message */
      break;
  }
}


void kinesis_video_init(CustomData *data, char *stream_name) {

  strcpy(data->stream_name, stream_name);
  data->stream_name[MAX_STREAM_NAME_LEN]='\0';
  LOG_INFO("kinesis_video_init enter data stream name" << data->stream_name);

  unique_ptr<DeviceInfoProvider> device_info_provider = make_unique<SampleDeviceInfoProvider>();
  unique_ptr<ClientCallbackProvider> client_callback_provider = make_unique<SampleClientCallbackProvider>();
  unique_ptr<StreamCallbackProvider> stream_callback_provider = make_unique<SampleStreamCallbackProvider>(data);

  char const *accessKey;
  char const *secretKey;
  char const *sessionToken;
  char const *defaultRegion;
  char const *iot_get_credential_endpoint;
  char const *cert_path;
  char const *private_key_path;
  char const *role_alias;
  char const *ca_cert_path;

  string defaultRegionStr;
  string sessionTokenStr;
  if (nullptr == (accessKey = getenv(ACCESS_KEY_ENV_VAR))) {
    accessKey = "AccessKey";
  }

  if (nullptr == (secretKey = getenv(SECRET_KEY_ENV_VAR))) {
    secretKey = "SecretKey";
  }

  if (nullptr == (sessionToken = getenv(SESSION_TOKEN_ENV_VAR))) {
    sessionTokenStr = "";
  } else {
    sessionTokenStr = string(sessionToken);
  }

  if (nullptr == (defaultRegion = getenv(DEFAULT_REGION_ENV_VAR))) {
    defaultRegionStr = DEFAULT_AWS_REGION;
  } else {
    defaultRegionStr = string(defaultRegion);
  }

  LOG_INFO("kinesis_video_init defaultRegion = " << defaultRegionStr);
  credentials_ = make_unique<Credentials>(string(accessKey),
                                          string(secretKey),
                                          sessionTokenStr,
                                          std::chrono::seconds(180));
  unique_ptr<CredentialProvider> credential_provider;
  if (nullptr!=(iot_get_credential_endpoint = getenv("IOT_GET_CREDENTIAL_ENDPOINT")) &&
      nullptr!=(cert_path = getenv("CERT_PATH")) &&
      nullptr!=(private_key_path = getenv("PRIVATE_KEY_PATH")) &&
      nullptr!=(role_alias = getenv("ROLE_ALIAS")) &&
      nullptr!=(ca_cert_path = getenv("CA_CERT_PATH"))) {
    LOG_INFO("Using IoT credentials for Kinesis Video Streams");
    credential_provider = make_unique<KvsSinkIotCertCredentialProvider>(iot_get_credential_endpoint,
                                                                        cert_path,
                                                                        private_key_path,
                                                                        role_alias,
                                                                        ca_cert_path,
                                                                        data->stream_name);

  } else {
    LOG_INFO("Using Sample credentials for Kinesis Video Streams");
    credential_provider = make_unique<SampleCredentialProvider>(*credentials_.get());
  }

  data->kinesis_video_producer = KinesisVideoProducer::createSync(move(device_info_provider),
                                                                  move(client_callback_provider),
                                                                  move(stream_callback_provider),
                                                                  move(credential_provider),
                                                                  defaultRegionStr);

  LOG_INFO("Kinesis Video Streams Client is ready");

  auto stream_definition = getStreamDefinitionPtr(data);
  data->kinesis_video_stream = data->kinesis_video_producer->createStreamSync(move(stream_definition));
  {
    std::lock_guard<std::mutex> lk(custom_data_mtx);
    data->connection_error = false;
  }

  LOG_INFO("Kinesis Video Stream is ready");
}

int gstreamer_init(int argc, char *argv[]) {

  /* Init GStreamer */
  gst_init(&argc, &argv);

  char stream_name[MAX_STREAM_NAME_LEN+1];
  strcpy(stream_name, argv[1]);
  g_print("Initializing GStreamer pipeline for KVS stream name=%s\n", stream_name);

  /* Initialize Kinesis Video */
  try {
    kinesis_video_init(&data, stream_name);
  } catch (runtime_error &err) {
    data.connection_error = true;
    LOG_ERROR("Time out error");
  }
  /* Create the elements */
  data.source = gst_element_factory_make("filesrc", "source");
  data.tsdemux = gst_element_factory_make("tsdemux", "tsdemux");
  data.h264parse = gst_element_factory_make("h264parse", "h264parse");
  data.filter = gst_element_factory_make("capsfilter", "filter");
  data.appsink = gst_element_factory_make("appsink", "appsink");

  /*
   *
   * gst-launch-1.0 filesrc -e location=20180731175011.ts ! tsdemux name=ts ! h264parse ! video/x-h264,stream-format=avc,alignment=au ! queue ! kvssink
   *
   */

  /* Create an empty pipeline */
  data.pipeline = gst_pipeline_new("fileingest-pipeline");

  g_object_set(G_OBJECT (data.tsdemux), "name", "ts", NULL);

  GstCaps *h264_caps = gst_caps_new_simple("video/x-h264",
                                           "stream-format", G_TYPE_STRING, "avc",
                                           "alignment", G_TYPE_STRING, "au",
                                           NULL);

  g_object_set(G_OBJECT (data.filter), "caps", h264_caps, NULL);
  gst_caps_unref(h264_caps);

  /* Configure source and appsink */

  g_object_set(G_OBJECT (data.source), "blocksize", 4096, NULL);

  g_object_set(G_OBJECT (data.appsink), "emit-signals", TRUE, "wait-on-eos", TRUE, NULL);
  g_object_set(G_OBJECT (data.appsink), "drop", TRUE, "max-buffers", 1, NULL);

  /* Build the pipeline */
  if (!data.pipeline || !data.source || !data.tsdemux || !data.h264parse || !data.filter || !data.appsink) {
    g_printerr("Not all elements could be created.\n");
    return 1;
  }
  gst_bin_add_many(GST_BIN (data.pipeline), data.source, data.tsdemux, data.h264parse, data.filter, data.appsink,
                   NULL);

  if (!gst_element_link_many(data.h264parse, data.filter, data.appsink, NULL)) {
    g_printerr("First Elements could not be linked.\n");
    gst_object_unref(data.pipeline);
    return 1;
  }

  if (!gst_element_link_many(data.source, data.tsdemux, NULL)) {
    g_printerr("Elements could not be linked.\n");
    gst_object_unref(data.pipeline);
    return 1;
  }

  /* Instruct the bus to emit signals for each received message, and connect to the interesting signals */
  data.bus = gst_element_get_bus(data.pipeline);
  gst_bus_add_signal_watch(data.bus);
  g_signal_connect(data.appsink, "new-sample", G_CALLBACK(on_new_sample), &data);
  g_signal_connect (G_OBJECT(data.bus), "message::error", (GCallback) error_cb, &data);
  g_signal_connect (G_OBJECT(data.bus), "message::eos", (GCallback) cb_message, &data);
  g_signal_connect(data.tsdemux, "pad-added", (GCallback) cb_ts_pad_created, &data);

  gst_object_unref(data.bus);
  g_print("gstreamer pipeline is ready and gstreamer_init complete.\n");
  return 0;
}

/* For every video clip set the  GStreamer pipeline to playing state */
int gstreamer_start(char *clip, char *levelclip) {
  GstStateChangeReturn ret;
  g_print("GStreamer_start enter : clip name is : %s : level file name is : %s clip queue size=%d\n", clip, levelclip, clip_queue.size());

  strcpy(gclip, clip);
  strcpy(glevelclip, levelclip);
  glevelclip[MAX_PATH_LEN]='\0';
  gclip[MAX_PATH_LEN]='\0';

  g_object_set(G_OBJECT (data.source), "location", clip, NULL);

  /* start streaming */
  ret = gst_element_set_state(data.pipeline, GST_STATE_PLAYING);
  if (ret == GST_STATE_CHANGE_FAILURE) {
    g_printerr("Unable to set the pipeline to the playing state.\n");
    goto CleanUp;
  }

  data.main_loop = g_main_loop_new(NULL, FALSE);
  g_main_loop_run(data.main_loop);
  LOG_INFO("Main loop in gstreamer_start finished");
  /* free resources */
  ret = gst_element_set_state(data.pipeline, GST_STATE_NULL);
  if (ret == GST_STATE_CHANGE_FAILURE) {
    g_printerr("Unable to set the pipeline to the NULL state after gstreamer_start_end\n");
    goto CleanUp;
  }
  g_print("gstreamer_start exit\n");
  return 0;

  CleanUp:
  if (data.main_loop !=  NULL) {
    g_main_loop_unref(data.main_loop);
  }
  return 1;
}

/* Note: The following stub is for testing purpose only */
/* In Production this will be replaced by the video clip generator */

/* TODO: Integrate lifecyle scenarios from customer and revise settings if needed */

int main(int argc, char *argv[]) {

  if (argc < 2 || strlen(argv[1]) > MAX_STREAM_NAME_LEN || strlen(argv[2]) > MAX_PATH_LEN ) {
    g_printerr(
        "\t\t Usage: \n \
         AWS_ACCESS_KEY_ID=SAMPLEKEY AWS_SECRET_ACCESS_KEY=SAMPLESECRET ./fileupload my-stream-name tsfilename \n  \
         or \n \
         CERT_PATH=PATH-TO-CERT CA_CERT_PATH=PATH-TO-CACERT PRIVATE_KEY_PATH=PATH-TO-PRIVATE-KEY ROLE_ALIAS=IOT-ROLE-ALIAS\n\
         IOT_GET_CREDENTIAL_ENDPOINT=CREDENTIAL_ENDPOINT ./fileupload my-stream-name tsfilename\n");
    return 1;
  }

  PropertyConfigurator::doConfigure(
      "kvs_log_configuration");

  /* init data struct */
  memset(&data, 0, sizeof(data));
  init_stats();
  data.frame_data = new uint8_t[DEFAULT_FRAME_DATA_SIZE_BYTE];

  bool connection_error = false;

  GstStateChangeReturn ret;
  gstreamer_init(argc, argv);

  while (true) {
    /*  Print the memory usage */
    compute_stats();

    {
      std::lock_guard<std::mutex> lk(custom_data_mtx);
      connection_error = data.connection_error;
    }

    /* Start streaming the video clip */
    LOG_DEBUG("Network connection error status = " << ((connection_error == 0) ? "NO_ERROR" : "ERROR" )  \
       << " clip queue size = " << clip_queue.size() << " "  \
       <<  (( clip_queue.size() < CLIP_QUEUE_SIZE_MAX_THRESHOLD) ? "(QUEUE_AVAILABLE)" : "(QUEUE_FULL)")  );

    if (!connection_error && clip_queue.size() < CLIP_QUEUE_SIZE_MAX_THRESHOLD) {
      g_print("Sending next clip");
      // customer uses two clips for local config but they send only one to KVS.
      // Note: for testing we do not unlink on every send call.
      clip_queue.add(argv[2]);
      int result = gstreamer_start(argv[2], argv[2]);

      if (result != 0) {
        g_printerr("Error in starting up GStreamer play for sending clips\n");
        goto Exit;
      }

    } else {

      /* in network error scenarios stop and free resources */
      if (data.kinesis_video_stream != NULL) {
        data.kinesis_video_stream->stopSync();
        data.kinesis_video_producer->freeStream(data.kinesis_video_stream);
        data.kinesis_video_stream = NULL;
      }

      ret = gst_element_set_state(data.pipeline, GST_STATE_NULL);
      if (ret != GST_STATE_CHANGE_FAILURE) {
        gst_object_unref(data.pipeline);
      } else {
        goto Exit;
      }
      std::this_thread::sleep_for(std::chrono::seconds(5));

      /* Delete older clips if there is a backlog beyond the threshold */
      g_print("Recreating pipelines\n");

      connection_error = false;
      gstreamer_init(argc, argv);
      LOG_DEBUG("Re-created network connection error status = " << ((connection_error == 0) ? "NO_ERROR" : "ERROR" )  \
       << " clip queue size = " << clip_queue.size() << " "  \
       <<  (( clip_queue.size() < CLIP_QUEUE_SIZE_MAX_THRESHOLD) ? "(QUEUE_AVAILABLE)" : "(QUEUE_FULL)")  );
    }
  }
  Exit:
  clip_queue.clear();
  close_stats();
  delete [] data.frame_data;
}

