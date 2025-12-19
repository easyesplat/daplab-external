#pragma once

#include "duckdb/common/common.hpp"
#include <string>
#include <unordered_map>

namespace duckdb {

class ExternalPragmaStorage {
public:
    static ExternalPragmaStorage& GetInstance() {
        static ExternalPragmaStorage instance;
        return instance;
    }
    
    void SetStreamEndpoint(const std::string& endpoint) {
        stream_endpoint = endpoint;
    }
    
    std::string GetStreamEndpoint() const {
        return stream_endpoint;
    }
    
    void SetPollingMs(idx_t ms) {
        polling_ms = ms;
    }
    
    idx_t GetPollingMs() const {
        return polling_ms;
    }
    
    void SetFinish(bool finish) {
        finished = finish;
    }
    
    bool GetFinish() const {
        return finished;
    }
    
private:
    // TODO(@eric): Stop doing a hacky fallback. Whole query should just fail if stream endpoint is
    // invalid. 
    std::string stream_endpoint = "/tmp/external_stream.txt";
    idx_t polling_ms = 100;
    bool finished = false;
};

} // namespace duckdb

