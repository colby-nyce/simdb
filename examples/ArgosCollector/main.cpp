#include "RandUtils.hpp"
#include "SimDBTester.hpp"
#include "simdb/apps/AppManager.hpp"
#include "simdb/apps/argos/Collection.hpp"
#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include "simdb/utils/Tree.hpp"

#include <vector>

/// This test shows how to use the SimDB data collection system for Argos.
TEST_INIT;

// Template specializations
namespace simdb::collection {

template <>
struct EnumDescriptor<simdb::Colors>
{
    static std::vector<EnumMember> members()
    {
        return {{"RED", 1},
                {"GREEN", 2},
                {"BLUE", 3},
                {"WHITE", 0},
                {"TRANSPARENT", -1}};
    }
};

} // namespace simdb::collection

/// Example simulator that configures all supported types of collections.
class Sim
{
public:
    void step()
    {
        randomizeDummyPacketCollectables_();
        auto tick = ++current_tick_;

        // Collect a random uint64_t between ticks 10 and 25
        if (tick == 1000)
        {
            // uint64_collectable_->activate(generateRandomInt<uint64_t>());
        } else if (tick == 2000)
        {
            // uint64_collectable_->deactivate();
        }

        // Collect a random bool between ticks 1500 and 2500
        if (tick == 1500)
        {
            // bool_collectable_->activate(rand() % 2 == 0);
        } else if (tick == 2500)
        {
            // bool_collectable_->deactivate();
        }

        // Collect a random enum between ticks 1800 and 2800
        if (tick == 1800)
        {
            // enum_collectable_->activate(generateRandomColor());
        } else if (tick == 2800)
        {
            // enum_collectable_->deactivate();
        }

        // Collect a random DummyPacket between ticks 2000 and 3000
        if (tick == 2000)
        {
            // dummy_packet_collectable_->activate(generateRandomDummyPacket());
        } else if (tick == 3000)
        {
            // dummy_packet_collectable_->deactivate();
        }

        // Collect some different values for just one cycle. To do this, we call
        // the activate() method, passing in "once=true".
        if (tick >= 5000 && tick % 5 == 0)
        {
            // uint64_collectable_->activate(generateRandomInt<uint64_t>(), true);
            // bool_collectable_->activate(rand() % 2 == 0, true);
            // enum_collectable_->activate(generateRandomColor(), true);
            // dummy_packet_collectable_->activate(generateRandomDummyPacket(), true);
        }

        // dummy_collectable_vec_contig_->activate(&dummy_packet_vec_contig_);
        // dummy_collectable_vec_sparse_->activate(&dummy_packet_vec_sparse_);
    }

    uint64_t getCurrentTick() const { return current_tick_; }

private:
    void randomizeDummyPacketCollectables_()
    {
        dummy_packet_vec_contig_.clear();
        for (int i = 0; i < rand() % 10; ++i)
        {
            dummy_packet_vec_contig_.push_back(simdb::generateRandomDummyPacket());
        }

        dummy_packet_vec_sparse_.clear();
        dummy_packet_vec_sparse_.resize(32);
        for (int i = 0; i < rand() % 10; ++i)
        {
            if (rand() % 2 == 0)
            {
                dummy_packet_vec_sparse_[i] = simdb::generateRandomDummyPacket();
            }
        }
    }

    uint64_t current_tick_ = 0;

    // std::shared_ptr<simdb::CollectionPoint> uint64_collectable_;
    // std::shared_ptr<simdb::CollectionPoint> bool_collectable_;
    // std::shared_ptr<simdb::CollectionPoint> enum_collectable_;
    // std::shared_ptr<simdb::CollectionPoint> dummy_packet_collectable_;

    simdb::DummyPacketPtrVec dummy_packet_vec_contig_;
    // std::shared_ptr<simdb::ContigIterableCollectionPoint> dummy_collectable_vec_contig_;

    simdb::DummyPacketPtrVec dummy_packet_vec_sparse_;
    // std::shared_ptr<simdb::SparseIterableCollectionPoint> dummy_collectable_vec_sparse_;
};

int main(int argc, char** argv)
{
    Sim sim;
    simdb::collection::Collection<uint64_t> collection;
    collection.addCollection("root", 1);
    collection.timestampWith([&sim]() { return sim.getCurrentTick(); });

    int intval = 5;
    auto auto_int_collector = collection.collectScalarWithAutoCollection<int>(
        "auto.int", "root", &intval);

    auto manual_int_collector = collection.collectScalarManually<int>(
        "manual.int", "root");

    auto color = simdb::Colors::GREEN;
    auto auto_enum_collector = collection.collectScalarWithAutoCollection<simdb::Colors>(
        "auto.color", "root", &color);

    auto manual_enum_collector = collection.collectScalarManually<simdb::Colors>(
        "manual.color", "root");

    class Packet
    {
    private:
        int intval_ = 8;
        std::string strval_ = "hello";
        simdb::Colors color_ = simdb::Colors::RED;

    public:
        int getInt() const { return intval_; }
        const std::string& getString() const { return strval_; }
        simdb::Colors getColor() const { return color_; }

        class ArgosCollector : public simdb::collection::ArgosCollectorBase<Packet>
        {
        public:
            ARGOS_COLLECT(intval, &Packet::getInt, "Signed integer payload sample");
            ARGOS_COLLECT(strval, &Packet::getString, "Short text label carried with the packet");
            ARGOS_COLLECT(color, &Packet::getColor, "Wire enum: simdb::Colors");
        };
    } packet;

    auto auto_packet_collector = collection.collectScalarWithAutoCollection<Packet>(
        "auto.packet", "root", &packet);

    auto manual_packet_collector = collection.collectScalarManually<Packet>(
        "manual.packet", "root");

    using PacketQueue = std::vector<std::shared_ptr<Packet>>;
    PacketQueue packet_queue;

    auto auto_packet_q_collector = collection.collectContainerWithAutoCollection<PacketQueue, false>(
        "auto.packet_q", "root", &packet_queue, 8);

    auto manual_packet_q_collector = collection.collectContainerManually<PacketQueue, false>(
        "manual.packet_q", "root", 8);

    simdb::AppManagers app_mgrs;
    app_mgrs.registerApp<simdb::collection::CollectionPipeline>();

    auto& app_mgr = app_mgrs.createAppManager("test.db");
    app_mgr.enableApp<simdb::collection::CollectionPipeline>();

    app_mgr.parameterizeAppFactory<simdb::collection::CollectionPipeline>(&collection);
    app_mgrs.createEnabledApps();
    app_mgrs.createSchemas();
    app_mgrs.postInit(argc, argv);
    app_mgrs.initializePipelines();
    app_mgrs.openPipelines();

    // TODO cnyce

    app_mgrs.postSimLoopTeardown();

    REPORT_ERROR;
    return ERROR_CODE;
}
