#pragma once
#include <unordered_map>

namespace sstables {
enum class target_directory_type { STANDARD };

class target_directories {
    // Not a multimap so we can efficiently access existing dirs by index
    struct directory_selector {
        unsigned index;
        std::vector<sstring> directories;
    };

    std::unordered_map<target_directory_type, directory_selector> _dirs;
public:
    sstring directory(target_directory_type t);
    void add_directory(target_directory_type t, sstring dir);
};
}
