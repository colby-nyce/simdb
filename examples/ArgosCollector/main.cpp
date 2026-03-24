#include "RandUtils.hpp"
#include "SimDBTester.hpp"
#include "simdb/apps/AppManager.hpp"
#include "simdb/apps/argos/Collections.hpp"

/// This test shows how to use the SimDB data collection system for Argos.
TEST_INIT;

/*
// Template specializations
namespace simdb {

template <> void defineEnumMap<Colors>(std::string& enum_name, std::map<std::string, int>& map)
{
    enum_name = "Colors";
    map["RED"] = 1;
    map["GREEN"] = 2;
    map["BLUE"] = 3;
    map["WHITE"] = 0;
    map["TRANSPARENT"] = -1;
}

template <> void defineStructSchema<DummyPacket>(StructSchema<DummyPacket>& schema)
{
    schema.addEnum<Colors>("color");
    schema.addField<char>("ch");
    schema.addField<int8_t>("int8");
    schema.addField<int16_t>("int16");
    schema.addField<int32_t>("int32");
    schema.addField<int64_t>("int64");
    schema.addField<uint8_t>("uint8");
    schema.addField<uint16_t>("uint16");
    schema.addField<uint32_t>("uint32");
    schema.addField<uint64_t>("uint64");
    schema.addField<float>("flt");
    schema.addField<double>("dbl");
    schema.addBool("b");
    schema.addString("str");
}

template <> void writeStructFields(const DummyPacket* pkt, StructFieldSerializer<DummyPacket>* serializer)
{
    serializer->writeField(pkt->e_color);
    serializer->writeField(pkt->ch);
    serializer->writeField(pkt->int8);
    serializer->writeField(pkt->int16);
    serializer->writeField(pkt->int32);
    serializer->writeField(pkt->int64);
    serializer->writeField(pkt->uint8);
    serializer->writeField(pkt->uint16);
    serializer->writeField(pkt->uint32);
    serializer->writeField(pkt->uint64);
    serializer->writeField(pkt->flt);
    serializer->writeField(pkt->dbl);
    serializer->writeField(pkt->b);
    serializer->writeField(pkt->str);
}

} // namespace simdb
*/

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
    simdb::collection::Collections collections;
    collections.addCollection<uint64_t>("root", 1);

    int intval = 5;
    auto auto_int_collector = collections.collectScalarWithAutoCollection<int>(
        "auto.int", "root", &intval);

    auto manual_int_collector = collections.collectScalarManually<int>(
        "manual.int", "root");

    struct Packet
    {
        int intval = 8;
        std::string strval = "hello";
        simdb::Colors color = simdb::Colors::RED;
    } packet;

    auto auto_packet_collector = collections.collectScalarWithAutoCollection<Packet>(
        "auto.packet", "root", &packet);

    auto manual_packet_collector = collections.collectScalarManually<Packet>(
        "manual.packet", "root");

    using PacketQueue = std::vector<std::shared_ptr<Packet>>;
    PacketQueue packet_queue;

    auto auto_packet_q_collector = collections.collectContainerWithAutoCollection<PacketQueue, false>(
        "auto.packet_q", "root", &packet_queue, 8);

    auto manual_packet_q_collector = collections.collectContainerManually<PacketQueue, false>(
        "manual.packet_q", "root", 8);

    simdb::AppManagers app_mgrs;
    app_mgrs.registerApp<simdb::collection::CollectionPipeline>();

    auto& app_mgr = app_mgrs.createAppManager("test.db");
    app_mgr.enableApp<simdb::collection::CollectionPipeline>();

    app_mgr.parameterizeAppFactory<simdb::collection::CollectionPipeline>(&collections);
    app_mgrs.createEnabledApps();
    app_mgrs.createSchemas();
    app_mgrs.postInit(argc, argv);
    app_mgrs.initializePipelines();
    app_mgrs.openPipelines();

    // TODO cnyce

    app_mgrs.postSimLoopTeardown();
}
