/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */

#include "sstable_deletion_manager.hh"
#include "sstables/remove.hh"
#include "log.hh"
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>
#include <unordered_map>

static logging::logger logger("sstable_deletion_manager");


class sstable_deletion_manager_shard;


class sstable_deletion_manager_shard
        : public seastar::async_sharded_service<sstable_deletion_manager_shard> {
    std::unordered_map<sstring, unsigned> _shards_allowing_delete;
public:
    future<> start() {
        return make_ready_future<>();
    }
    future<> stop() {
        // FIXME: wait for ongoing deletions
        return make_ready_future<>();
    }
    future<> shard_may_delete(unsigned shard_id, sstring sstable_toc_name) {
        auto& counter = _shards_allowing_delete[sstable_toc_name];
        counter += 1;
        logger.trace("shard {} allows deletion of {}; {} now agree", shard_id, sstable_toc_name, counter);
        if (counter < smp::count) {
            return make_ready_future<>();
        }
        _shards_allowing_delete.erase(sstable_toc_name);
        logger.info("removing shared sstable {}", sstable_toc_name);
        return sstables::remove_by_toc_name(sstable_toc_name).then([me = shared_from_this()] {});
    }
};

class sstable_deletion_manager::impl {
public:
    virtual ~impl() {}
    virtual future<> start() = 0;
    virtual future<> stop() = 0;
    virtual void may_delete(sstring sstable_toc_name, bool is_shared) = 0;
};

class sstable_deletion_manager::real_impl : public impl {
    // _shards is self-guarding wrt async operations; but we still need to protect
    // the containing object (and we do that with _gate)
    seastar::sharded<sstable_deletion_manager_shard> _shards;
    seastar::gate _gate;
public:
    virtual future<> start() override {
        return _shards.start();
    }
    virtual future<> stop() override {
        return _gate.close().then([this] {
            return _shards.stop();
        });
    }
    virtual void may_delete(sstring sstable_toc_name, bool is_shared) override {
        seastar::with_gate(_gate, [this, sstable_toc_name, is_shared] {
            if (is_shared && smp::count > 1) {
                logger.info("scheduling shared sstable {} for deletion", sstable_toc_name);
                auto owner = std::hash<sstring>()(sstable_toc_name) % smp::count;
                return _shards.invoke_on(owner,
                        &sstable_deletion_manager_shard::shard_may_delete,
                        engine().cpu_id(), sstable_toc_name);
            } else {
                logger.info("removing shard-exclusive sstable {}", sstable_toc_name);
                return sstables::remove_by_toc_name(sstable_toc_name);
            }
        });
    }
};

class sstable_deletion_manager::dummy_impl : public impl {
    seastar::sharded<sstable_deletion_manager_shard> _shards;
    seastar::gate _gate;
public:
    virtual future<> start() override {
        return make_ready_future<>();
    }
    virtual future<> stop() override {
        return make_ready_future<>();
    }
    virtual void may_delete(sstring sstable_toc_name, bool is_shared) override {
    }
};

sstable_deletion_manager::sstable_deletion_manager(std::unique_ptr<impl> impl)
        : _impl(std::move(impl)) {
}

sstable_deletion_manager::sstable_deletion_manager(sstable_deletion_manager&&) noexcept = default;

sstable_deletion_manager::~sstable_deletion_manager() = default;

future<>
sstable_deletion_manager::start() {
    return _impl->start();
}

future<>
sstable_deletion_manager::stop() {
    return _impl->stop();
}

void
sstable_deletion_manager::may_delete(sstring sstable_toc_name, bool is_shared) {
    _impl->may_delete(sstable_toc_name, is_shared);
}

sstable_deletion_manager make_sstable_deletion_manager() {
    return sstable_deletion_manager(std::make_unique<sstable_deletion_manager::real_impl>());
}

sstable_deletion_manager make_dummy_sstable_deletion_manager() {
    return sstable_deletion_manager(std::make_unique<sstable_deletion_manager::dummy_impl>());
}
