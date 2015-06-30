/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>

#include "tests/test-utils.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"
#include "core/scollectd_api.hh"
#include "core/file.hh"
#include "core/reactor.hh"
#include "utils/UUID_gen.hh"
#include "db/commitlog/commitlog.hh"
#include "db/serializer.hh"

using namespace db;

// temp commitlog dir.
struct tmpdir {
    tmpdir() {
        char tmp[16] = "commitlogXXXXXX";
        auto * dir = ::mkdtemp(tmp);
        if (dir == NULL) {
            throw std::runtime_error("Could not create temp dir");
        }
        path = dir;
        //std::cout << path << std::endl;
    }
    tmpdir(tmpdir&& v)
            : path(std::move(v.path)) {
        assert(v.path.empty());
    }
    tmpdir(const tmpdir&) = delete;
    ~tmpdir() {
        if (!path.empty()) {
            // deal with non-empty?
            ::rmdir(path.c_str());
        }
    }
    tmpdir & operator=(tmpdir&& v) {
        if (&v != this) {
            this->~tmpdir();
            new (this) tmpdir(std::move(v));
        }
        return *this;
    }
    tmpdir & operator=(const tmpdir&) = delete;
    sstring path;
};

typedef std::pair<tmpdir, commitlog> tmplog;
typedef lw_shared_ptr<tmplog> tmplog_ptr;

// create tmp dir + commit log
static future<tmplog_ptr> make_commitlog(commitlog::config cfg =
        commitlog::config()) {
    tmpdir tmp;
    cfg.commit_log_location = tmp.path;
    return commitlog::create_commitlog(cfg).then(
            [tmp = std::move(tmp)](commitlog log) mutable {
                return make_ready_future<tmplog_ptr>(make_lw_shared<tmplog>(std::move(tmp), std::move(log)));
            });
}

class file_lister {
    file _f;
    subscription<directory_entry> _listing;
public:
    file_lister(file f)
            : _f(std::move(f)), _listing(
                    _f.list_directory(
                            [this] (directory_entry de) {return report(de);})) {
    }
    future<> done() {
        return _listing.done();
    }
    const std::vector<directory_entry> & contents() const {
        return _contents;
    }
private:
    std::vector<directory_entry> _contents;

    future<> report(directory_entry de) {
        _contents.emplace_back(de);
        return make_ready_future<>();
    }
};

static future<lw_shared_ptr<file_lister>> list_files(sstring path) {
    return engine().open_directory(path).then([](auto dir) {
        auto l = make_lw_shared<file_lister>(std::move(dir));
        return l->done().then([l]() {
            return make_ready_future<lw_shared_ptr<file_lister>>(l);
        });
    });
}

static future<size_t> count_files(sstring path) {
    return list_files(path).then([](auto l) {
        size_t n = 0;
        for (auto & de : l->contents()) {
            if (de.type == directory_entry_type::regular) {
                ++n;
            }
        }
        return make_ready_future<size_t>(n);
    });
}

static future<size_t> count_files_with_size(sstring path) {
    return list_files(path).then([path](auto l) {
        auto n = make_lw_shared<size_t>(0);
        return parallel_for_each(l->contents().begin(), l->contents().end(), [n, path](directory_entry de) {
            if (de.type == directory_entry_type::regular) {
                return engine().open_file_dma(path + "/" + de.name, open_flags::ro).then([n](file f) {
                    return do_with(std::move(f), [n] (auto& f) {
                        return f.stat().then([n](struct stat s) {
                            if (s.st_size > 0) {
                                ++(*n);
                            }
                        });
                    });
                });
            }
            return make_ready_future();
        }).then([n]() {
           return make_ready_future<size_t>(*n);;
        });
    });
}

namespace db {
template<typename... Args>
inline std::basic_ostream<Args...> & operator<<(std::basic_ostream<Args...> & os, const db::replay_position & rp) {
    return os << "[" << rp.id << ", " << rp.pos << "]" << std::endl;

}
}
// just write in-memory...
SEASTAR_TEST_CASE(test_create_commitlog){
    return make_commitlog().then([](tmplog_ptr log) {
            sstring tmp = "hej bubba cow";
            return log->second.add_mutation(utils::UUID_gen::get_time_UUID(), tmp.size(), [tmp](db::commitlog::output& dst) {
                        dst.write(tmp.begin(), tmp.end());
                    }).then([](db::replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        });
}

// check we
SEASTAR_TEST_CASE(test_commitlog_written_to_disk_batch){
    commitlog::config cfg;
    cfg.mode = commitlog::sync_mode::BATCH;
    return make_commitlog(cfg).then([](tmplog_ptr log) {
            sstring tmp = "hej bubba cow";
            return log->second.add_mutation(utils::UUID_gen::get_time_UUID(), tmp.size(), [tmp](db::commitlog::output& dst) {
                        dst.write(tmp.begin(), tmp.end());
                    }).then([log](replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                        return count_files_with_size(log->first.path).then([log](size_t n) {
                                    BOOST_REQUIRE(n > 0);
                                });
                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_written_to_disk_periodic){
    return make_commitlog().then([](tmplog_ptr log) {
            auto state = make_lw_shared(false);
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([state]() {return *state;},
                    [log, state, uuid]() {
                        sstring tmp = "hej bubba cow";
                        return log->second.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.begin(), tmp.end());
                                }).then([log, state](replay_position rp) {
                                    BOOST_CHECK_NE(rp, db::replay_position());
                                    return count_files_with_size(log->first.path).then([state](size_t n) {
                                       *state = n > 0;
                                    });
                                });

                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_new_segment){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return make_commitlog(cfg).then([](tmplog_ptr log) {
            auto state = make_lw_shared(false);
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([state]() {return *state;},
                    [log, state, uuid]() {
                        sstring tmp = "hej bubba cow";
                        return log->second.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.begin(), tmp.end());
                                }).then([log, state](replay_position rp) {
                                    BOOST_CHECK_NE(rp, db::replay_position());
                                    *state = rp.id > 1;
                                });

                    }).then([log]() {
                        return count_files(log->first.path).then([](size_t n) {
                                    BOOST_REQUIRE(n > 1);
                                });
                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        }).then([]{});
}


SEASTAR_TEST_CASE(test_commitlog_discard_completed_segments){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return make_commitlog(cfg).then([](tmplog_ptr log) {
            struct state_type {
                std::vector<utils::UUID> uuids;
                std::unordered_map<utils::UUID, replay_position> rps;
                mutable size_t index = 0;
                bool done = false;

                state_type() {
                    for (int i = 0; i < 10; ++i) {
                        uuids.push_back(utils::UUID_gen::get_time_UUID());
                    }
                }
                const utils::UUID & next_uuid() const {
                    return uuids[index++ % uuids.size()];
                }
            };

            auto state = make_lw_shared<state_type>();
            return do_until([state]() {return state->done;},
                    [log, state]() {
                        sstring tmp = "hej bubba cow";
                        auto uuid = state->next_uuid();
                        return log->second.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.begin(), tmp.end());
                                }).then([log, state, uuid](replay_position pos) {
                                    state->done = pos.id > 1;
                                    state->rps[uuid] = pos;
                                });
                    }).then([log, state]() {
                        return count_files(log->first.path).then([log, state](size_t n) {
                                    BOOST_REQUIRE(n > 1);
                                    for (auto & p : state->rps) {
                                        log->second.discard_completed_segments(p.first, p.second);
                                    }
                                    return count_files(log->first.path).then([n](size_t nn) {
                                                BOOST_REQUIRE(nn > 0);
                                                BOOST_REQUIRE(nn < n);
                                            });
                                });
                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        });
}

SEASTAR_TEST_CASE(test_equal_record_limit){
    return make_commitlog().then([](tmplog_ptr log) {
            auto size = log->second.max_record_size();
            return log->second.add_mutation(utils::UUID_gen::get_time_UUID(), size, [size](db::commitlog::output& dst) {
                        dst.write(char(1), size);
                    }).then([](db::replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        });
}

SEASTAR_TEST_CASE(test_exceed_record_limit){
    return make_commitlog().then([](tmplog_ptr log) {
            auto size = log->second.max_record_size() + 1;
            return log->second.add_mutation(utils::UUID_gen::get_time_UUID(), size, [size](db::commitlog::output& dst) {
                        dst.write(char(1), size);
                    }).then([](db::replay_position rp) {
                        // should not reach.
                    }).then_wrapped([](future<> f) {
                        try {
                            f.get();
                        } catch (...) {
                            // ok.
                            return make_ready_future();
                        }
                        throw std::runtime_error("Did not get expected exception from writing too large record");
                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_delete_when_over_disk_limit){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 2;
    cfg.commitlog_total_space_in_mb = 1;
    return make_commitlog(cfg).then([](tmplog_ptr log) {
            auto state = make_lw_shared(false);
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([state]() {return *state;},
                    [log, state, uuid]() {
                        sstring tmp = "hej bubba cow";
                        return log->second.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.begin(), tmp.end());
                                }).then([log, state](replay_position rp) {
                                    BOOST_CHECK_NE(rp, db::replay_position());
                                    *state = rp.id > 1;
                                });

                    }).then([log]() {
                        return count_files(log->first.path).then([](size_t n) {
                                    BOOST_REQUIRE(n > 0);
                                    BOOST_REQUIRE(n < 2);
                                });
                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        }).then([]{});
}

SEASTAR_TEST_CASE(test_commitlog_reader){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return make_commitlog(cfg).then([](tmplog_ptr log) {
            auto state = make_lw_shared(false);
            auto count = make_lw_shared<size_t>(0);
            auto count2 = make_lw_shared<size_t>(0);
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([state]() {return *state;},
                    [log, state, uuid, count]() {
                        sstring tmp = "hej bubba cow";
                        return log->second.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.begin(), tmp.end());
                                }).then([log, state, count](replay_position rp) {
                                    BOOST_CHECK_NE(rp, db::replay_position());
                                    if (rp.id == 1) {
                                        (*count)++;
                                    }
                                    *state = rp.id > 1;
                                });

                    }).then([log, count]() {
                        return count_files(log->first.path).then([](size_t n) {
                                    BOOST_REQUIRE(n > 1);
                                });
                    }).then([log, count2] {
                        return list_files(log->first.path).then([log, count2](auto l) {
                                    for (auto & de : l->contents()) {
                                        // TODO, meh, hard coded name...
                                        if (de.name == "CommitLog-1-1.log") {
                                            auto path = log->first.path + "/" + de.name;
                                            return db::commitlog::read_log_file(path, [count2](temporary_buffer<char> buf) {
                                                        sstring str(buf.get(), buf.size());
                                                        BOOST_CHECK_EQUAL(str, "hej bubba cow");
                                                        (*count2)++;
                                                        return make_ready_future<>();
                                                    }).then([log](subscription<temporary_buffer<char>> s) {
                                                        auto ss = make_lw_shared(std::move(s));
                                                        return ss->done().then([ss] {});
                                                    });
                                        }
                                    }
                                    throw std::runtime_error("Did not find expected log file");
                               });
                    }).then([count, count2] {
                        BOOST_CHECK_EQUAL(*count, *count2);
                    }).finally([log]() {
                        return log->second.clear().then([log] {});
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_counters) {
    auto count_cl_counters = []() -> size_t {
        auto ids = scollectd::get_collectd_ids();
        return std::count_if(ids.begin(), ids.end(), [](const scollectd::type_instance_id& id) {
            return id.plugin() == "commitlog";
        });
    };
    BOOST_CHECK_EQUAL(count_cl_counters(), 0);
    return make_commitlog().then([&](tmplog_ptr log) {
        BOOST_CHECK_GT(count_cl_counters(), 0);
    }).finally([&]() {
        BOOST_CHECK_EQUAL(count_cl_counters(), 0);
    });
}


