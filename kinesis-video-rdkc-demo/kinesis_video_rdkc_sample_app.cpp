#include <string.h>
#include <chrono>
#include <Logger.h>
#include "KinesisVideoProducer.h"
#include <vector>
#include <stdlib.h>
#include <glib.h>

using namespace std;
using namespace com::amazonaws::kinesis::video;
using namespace log4cplus;

#ifdef __cplusplus
extern "C" {
#endif
#include "rdkc_stream_api.h"

int rdkcstreamer_init(int, char **);

#ifdef __cplusplus
}
#endif

LOGGER_TAG("com.amazonaws.kinesis.video.rdkcstreamer");

#define ACCESS_KEY_ENV_VAR "AWS_ACCESS_KEY_ID"
#define SECRET_KEY_ENV_VAR "AWS_SECRET_ACCESS_KEY"
#define SESSION_TOKEN_ENV_VAR "AWS_SESSION_TOKEN"
#define DEFAULT_REGION_ENV_VAR "AWS_DEFAULT_REGION"

FILE *fp = NULL;

namespace com { namespace amazonaws { namespace kinesis { namespace video {

class SampleClientCallbackProvider : public ClientCallbackProvider {
public:

    StorageOverflowPressureFunc getStorageOverflowPressureCallback() override {
        return storageOverflowPressure;
    }

    static STATUS storageOverflowPressure(UINT64 custom_handle, UINT64 remaining_bytes);
};

class SampleStreamCallbackProvider : public StreamCallbackProvider {
public:

    StreamConnectionStaleFunc getStreamConnectionStaleCallback() override {
        return streamConnectionStaleHandler;
    };

    StreamErrorReportFunc getStreamErrorReportCallback() override {
        return streamErrorReportHandler;
    };

    DroppedFrameReportFunc getDroppedFrameReportCallback() override {
        return droppedFrameReportHandler;
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
        // Set the storage size to 256mb
        device_info.storageInfo.storageSize = 10 * 1024 * 1024;
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
    return STATUS_SUCCESS;
}

STATUS
SampleStreamCallbackProvider::droppedFrameReportHandler(UINT64 custom_data, STREAM_HANDLE stream_handle,
                                                        UINT64 dropped_frame_timecode) {
    LOG_WARN("Reporting dropped frame. Frame timecode " << dropped_frame_timecode);
    return STATUS_SUCCESS;
}

}  // namespace video
}  // namespace kinesis
}  // namespace amazonaws
}  // namespace com;

unique_ptr<Credentials> credentials_;

typedef struct _CustomData {
    unique_ptr<KinesisVideoProducer> kinesis_video_producer;
    shared_ptr<KinesisVideoStream> kinesis_video_stream;
    bool stream_started;
    bool h264_stream_supported;
} CustomData;


void create_kinesis_video_frame(Frame *frame, const nanoseconds &pts, const nanoseconds &dts, FRAME_FLAGS flags,
                                void *data, size_t len) {
    frame->flags = flags;
    frame->decodingTs = static_cast<UINT64>(dts.count()) / DEFAULT_TIME_UNIT_IN_NANOS;
    frame->presentationTs = static_cast<UINT64>(pts.count()) / DEFAULT_TIME_UNIT_IN_NANOS;
    frame->duration = 10 * HUNDREDS_OF_NANOS_IN_A_MILLISECOND;
    frame->size = static_cast<UINT32>(len);
    frame->frameData = reinterpret_cast<PBYTE>(data);
}

bool put_frame(shared_ptr<KinesisVideoStream> kinesis_video_stream, void *data, size_t len, const nanoseconds &pts, const nanoseconds &dts, FRAME_FLAGS flags) {
    Frame frame;
    create_kinesis_video_frame(&frame, pts, dts, flags, data, len);
    return kinesis_video_stream->putFrame(frame);
}

static int on_new_sample(CustomData *data, RDKC_FrameInfo *pconfig, int &dumpstream) {
    //usleep(66000);

    /*printf("(%d): on_new_sample : stream_type=%d, frame_size=%d, pic_type=%d, frame_num=%d, width=%d, height=%d, timestamap=%d, arm_pts=%llu! ,dsp_pts=%llu!\n", 
        __LINE__, pconfig->stream_type, pconfig->frame_size, pconfig->pic_type, pconfig->frame_num, pconfig->width, pconfig->height, 
        pconfig->frame_timestamp, pconfig->arm_pts, pconfig->dsp_pts);*/
    
    if (dumpstream == 1) {
        int write_bytes = 0;
        int ret = 0;
        
        if( NULL != fp ) {
            //  read video frame, write into file
            write_bytes = fwrite((char *)pconfig->frame_ptr, pconfig->frame_size, 1, fp);
            if (write_bytes <= 0)
            {
                printf("(%d): write video error. ret=%d, errmsg(%d)=%s!\n", __LINE__, ret, errno, strerror(errno));
                ret = -1;
                return ret;
            }
        }
    }

    data->h264_stream_supported = true;
    if (!data->stream_started) {
        data->stream_started = true;
        data->kinesis_video_stream->start(std::string("014d001fffe1002e674d001f9a6402802dff80b5010101400000fa40003a983a18005b900005b8e2ef2e343000b720000b71c5de5c2801000468ee3c80"));
    }

    FRAME_FLAGS kinesis_video_flags;
    uint8_t *frame_data = (uint8_t*) pconfig->frame_ptr;
    size_t buffer_size = (size_t) pconfig->frame_size;

    if ( pconfig->pic_type == 1 ) {
        kinesis_video_flags = FRAME_FLAG_KEY_FRAME;
    } else {
        kinesis_video_flags = FRAME_FLAG_NONE;
    }

    std::chrono::nanoseconds frametimestamp_nano = std::chrono::milliseconds(pconfig->frame_timestamp);
    
    //printf("Frame timestamp in milliseconds : %d\n",pconfig->frame_timestamp);
    //printf("Frame timestamp in nanoseconds : %llu\n",frametimestamp_nano);
    printf("frame_data size : %d\n",buffer_size);
    printf("\n------------frame_data Data Start------------\n");
    for ( int i=0 ; i < 10 ; i++ ) {
        printf("%02x\t",frame_data[i]);
    }
    printf("\n------------frame_data Data End------------\n");

    if (false == put_frame(data->kinesis_video_stream, frame_data, buffer_size, frametimestamp_nano,
                           frametimestamp_nano, kinesis_video_flags)) {
        g_printerr("Dropped frame!\n");
    }
    
    return 0;
}

static volatile sig_atomic_t term_flag = 0;
void term_streaming(int sig)
{
	term_flag = 1;
}

int rdkc_stream_init_read_frame(CustomData *data, int &dumpstream) { 
    int ret = 0;
    int stream_id = 0;
    int video_stream_fd = 0;
    StreamConfig config;
    RDKC_FrameInfo frame_info;

    if( dumpstream == 1) {
        fp = fopen("/opt/video.H264", "wo+");
        if (NULL == fp)
        {
            ret = -1;
            printf("%d:open video file error\n",__LINE__);
            goto func_exit;
        }
    }

    // Get stream config
	memset(&config, 0, sizeof(StreamConfig));
    rdkc_stream_get_config(stream_id, &config);
    printf("(%d): stream_type=%d!\n", __LINE__, config.stream_type);
    printf("(%d): width=%d!\n", __LINE__, config.width);
    printf("(%d): height=%d!\n", __LINE__, config.height);
    printf("(%d): frame_rate=%d!\n", __LINE__, config.frame_rate);
    printf("(%d): gov_length=%d!\n", __LINE__, config.gov_length);
    printf("(%d): profile=%d!\n", __LINE__, config.profile);
    printf("(%d): quality_type=%d!\n", __LINE__, config.quality_type);
    printf("(%d): quality_level=%d!\n", __LINE__, config.quality_level);
    printf("(%d): bit_rate=%d!\n", __LINE__, config.bit_rate);

    ret = rdkc_stream_set_config(stream_id, &config);
    if (ret < 0)
    {
        printf("rdkc_stream_set_config error!\n");
        ret = -1;
        goto func_exit;
    }
        
    rdkc_stream_get_config(stream_id, &config);
    printf("(%d): after set : dump stream flag=%d!\n", __LINE__, dumpstream);
    printf("(%d): after set : stream_type=%d!\n", __LINE__, config.stream_type);
    printf("(%d): after set : width=%d!\n", __LINE__, config.width);
    printf("(%d): after set : height=%d!\n", __LINE__, config.height);
    printf("(%d): after set : frame_rate=%d!\n", __LINE__, config.frame_rate);
    printf("(%d): after set : gov_length=%d!\n", __LINE__, config.gov_length);
    printf("(%d): after set : profile=%d!\n", __LINE__, config.profile);
    printf("(%d): after set : quality_type=%d!\n", __LINE__, config.quality_type);
    printf("(%d): after set : quality_level=%d!\n", __LINE__, config.quality_level);
    printf("(%d): after set : bit_rate=%d!\n", __LINE__, config.bit_rate);

    printf("(%d): Set stream settings sucessfully! Wait few seconds for streaming server reload.\n", __LINE__);
    sleep(4);

    video_stream_fd = rdkc_stream_init(stream_id,1);//video
    if (video_stream_fd < 0)
    {
        printf("Init rdkc stream video error!\n");
        ret = -1;
        goto func_exit;
    }

    while (!term_flag)
    {
        ret = rdkc_stream_read_frame(video_stream_fd, &frame_info);
        if (0 == ret)
        {
            /*printf("(%d): rdkc_stream_init_read_frame : Got one frame! stream_type=%d, frame_size=%d, pic_type=%d, frame_num=%d, width=%d, height=%d, timestamap=%d, arm_pts=%llu, dsp_pts=%llu!\n", 
                __LINE__, frame_info.stream_type, frame_info.frame_size, frame_info.pic_type, frame_info.frame_num, frame_info.width, frame_info.height, 
                frame_info.frame_timestamp, frame_info.arm_pts, frame_info.dsp_pts);*/
            on_new_sample(data,&frame_info,dumpstream);
        }
        else if (1 == ret)
        {
            // No frame data ready, try again
            //printf("(%d):Video No frame data ready, try again.\n", __LINE__);
            usleep(10000);
            continue;
        }
        else
        {
            printf("(%d):Video Read rdkc stream error.\n", __LINE__);
            ret = -1;
            goto func_exit;
        }
    }

func_exit:
    printf("(%d): Rdkc stream close.\n", __LINE__);
    if( dumpstream == 1) {
        if (fp)
        {
            fclose(fp);
            fp = NULL;
        }
    }

    if( NULL != video_stream_fd ) {
        rdkc_stream_close(video_stream_fd);
    }
    return ret;
}

void kinesis_video_init(CustomData *data, char *stream_name) {
    unique_ptr<DeviceInfoProvider> device_info_provider = make_unique<SampleDeviceInfoProvider>();
    unique_ptr<ClientCallbackProvider> client_callback_provider = make_unique<SampleClientCallbackProvider>();
    unique_ptr<StreamCallbackProvider> stream_callback_provider = make_unique<SampleStreamCallbackProvider>();

    char const *accessKey;
    char const *secretKey;
    char const *sessionToken;
    char const *defaultRegion;
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

    credentials_ = make_unique<Credentials>(string(accessKey),
                                            string(secretKey),
                                            sessionTokenStr,
                                            std::chrono::seconds(180));
    unique_ptr<CredentialProvider> credential_provider = make_unique<SampleCredentialProvider>(*credentials_.get());

    data->kinesis_video_producer = KinesisVideoProducer::createSync(move(device_info_provider),
                                                                    move(client_callback_provider),
                                                                    move(stream_callback_provider),
                                                                    move(credential_provider),
                                                                    defaultRegionStr);

    LOG_DEBUG("Client is ready");
    /* create a test stream */
    map<string, string> tags;
    char tag_name[MAX_TAG_NAME_LEN];
    char tag_val[MAX_TAG_VALUE_LEN];
    sprintf(tag_name, "piTag");
    sprintf(tag_val, "piValue");
    auto stream_definition = make_unique<StreamDefinition>(stream_name,
                                                           hours(24),
                                                           &tags,
                                                           "",
                                                           STREAMING_TYPE_REALTIME,
                                                           "video/h264",
                                                           milliseconds::zero(),
                                                           seconds(2),
                                                           milliseconds(1),
                                                           true,//Construct a fragment at each key frame
                                                           true,//Use provided frame timecode
                                                           false,//Relative timecode
                                                           true,//Ack on fragment is enabled
                                                           true,//SDK will restart when error happens
                                                           true,//recalculate_metrics
                                                           NAL_ADAPTATION_ANNEXB_NALS,
                                                           15,
                                                           4 * 1024 * 1024,
                                                           seconds(120),
                                                           seconds(40),
                                                           seconds(30),
                                                           "V_MPEG4/ISO/AVC",
                                                           "kinesis_video",
                                                           nullptr,
                                                           0);
    data->kinesis_video_stream = data->kinesis_video_producer->createStreamSync(move(stream_definition));

    LOG_DEBUG("Stream is ready");
}

int rdkcstreamer_init(int argc, char* argv[]) {
    BasicConfigurator config;
    config.configure();

    if (argc < 2) {
        LOG_ERROR(
                "Usage: AWS_ACCESS_KEY_ID=SAMPLEKEY AWS_SECRET_ACCESS_KEY=SAMPLESECRET ./kinesis_video_rdkc_sample_app my-stream-name -w width -h height -f framerate -b bitrateInKBPS -d enabledumpstream<0/1>");
        return 1;
    }

    CustomData data;
    bool vtenc, isOnRpi;

    /* init data struct */
    memset(&data, 0, sizeof(data));

    /* init stream format */
    int opt, width = 0, height = 0, framerate=30, bitrateInKBPS=512, dumpstream=0;
    char *endptr;
    while ((opt = getopt(argc, argv, "w:h:f:b:d:")) != -1) {
        switch (opt) {
        case 'w':
            width = strtol(optarg, &endptr, 0);
            if (*endptr != '\0') {
                g_printerr("Invalid width value.\n");
                return 1;
            }
            break;
        case 'h':
            height = strtol(optarg, &endptr, 0);
            if (*endptr != '\0') {
                g_printerr("Invalid height value.\n");
                return 1;
            }
            break;
        case 'f':
            framerate = strtol(optarg, &endptr, 0);
            if (*endptr != '\0') {
                g_printerr("Invalid framerate value.\n");
                return 1;
            }
            break;
        case 'b':
            bitrateInKBPS = strtol(optarg, &endptr, 0);
            if (*endptr != '\0') {
                g_printerr("Invalid bitrate value.\n");
                return 1;
            }
            break;
        case 'd':
            dumpstream = strtol(optarg, &endptr, 0);
            if (*endptr != '\0') {
                g_printerr("Invalid dump stream value.\n");
                return 1;
            }
            break;
        default: /* '?' */
            g_printerr("Invalid arguments\n");
            return 1;
        }
    }

    /* init Kinesis Video */
    char stream_name[MAX_STREAM_NAME_LEN];
    SNPRINTF(stream_name, MAX_STREAM_NAME_LEN, argv[optind]);
    kinesis_video_init(&data, stream_name);

    if ((width == 0 && height != 0) || (width != 0 && height == 0)) {
        g_printerr("Invalid resolution\n");
        return 1;
    }

    rdkc_stream_init_read_frame(&data,dumpstream);
    return 0;
}

int main(int argc, char* argv[]) {
    signal(SIGTERM, term_streaming);
    return rdkcstreamer_init(argc, argv);
}
