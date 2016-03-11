#include <core/reactor.hh>
#include <seastar/core/sstring.hh>
#include "tmpdir.hh"
#include <seastar/tests/test-utils.hh>
#include <boost/range/irange.hpp>

SEASTAR_TEST_CASE(reproducer) {
    printf("Starting test\n");
    auto tmp = make_lw_shared<tmpdir>();
    return do_with(int(0), [tmp] (int& finished) {
        size_t count = 500;
        auto idx = boost::irange(0, int(count));
        printf("Shard %d start writing %ld files \n", engine().cpu_id(), count);
        return do_for_each(idx.begin(), idx.end(), [&finished, tmp] (uint64_t idx) {
            auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
            auto name = sprint("%s/test-file-%ld",tmp->path, idx);
            return open_file_dma(name, oflags).then([] (auto f) {
                    return f.truncate(4096).then([f] () mutable {
                }).then([f] () mutable {
                    auto bufptr = allocate_aligned_buffer<char>(4096, 4096);
                    auto buf = bufptr.get();
                    return f.dma_write(0, buf, 4096).then([bufptr = std::move(bufptr)] (auto s) {});
                }).then([f] () mutable {
                    return f.flush();
                }).then([f] {});
            });
        }).then([count] {
             printf("Shard %d finished writing %ld files\n", engine().cpu_id(), count);
             return make_ready_future<>();
        });
    }).then([tmp] {});
}

