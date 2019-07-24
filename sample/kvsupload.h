#ifndef _KVS_UPLOAD_API_H
#define _KVS_UPLOAD_API_H

//init kvs sdk
int kvs_init();

//Api's for stream uploads
int kvs_stream_init(unsigned short& audioenabled, unsigned short& abstimestamp, unsigned short& livemode, unsigned short& contentchangestatus, uint64_t& hangdetecttime);
int kvs_stream_play(char* cliplocation,unsigned short& audioenabled, unsigned short& abstimestamp, unsigned short& livemode, uint64_t& hangdetecttime);

#endif
