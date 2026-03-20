#include "SimDBTester.hpp"
#include "simdb/apps/AppManager.hpp"
#include "simdb/apps/argos/Collections.hpp"

#include <random>
std::random_device rd;  // Seed source for the random number engine
std::mt19937 gen(rd()); // mersenne_twister_engine

/// This test shows how to use the SimDB data collection system for Argos.
TEST_INIT;

/// Enum used to verify TinyStrings
enum class Colors { RED = 1, GREEN = 2, BLUE = 3, WHITE = 0, TRANSPARENT = -1 };

inline std::ostream& operator<<(std::ostream& os, const Colors& c)
{
    switch (c)
    {
    case Colors::RED:
        os << "RED";
        break;
    case Colors::GREEN:
        os << "GREEN";
        break;
    case Colors::BLUE:
        os << "BLUE";
        break;
    case Colors::WHITE:
        os << "WHITE";
        break;
    case Colors::TRANSPARENT:
        os << "TRANSPARENT";
        break;
    default:
        os << "UNKNOWN";
        break;
    }
    return os;
}

/// Example struct that contains a wide variety of supported data fields
struct DummyPacket
{
    Colors e_color;
    char ch;
    int8_t int8;
    int16_t int16;
    int32_t int32;
    int64_t int64;
    uint8_t uint8;
    uint16_t uint16;
    uint32_t uint32;
    uint64_t uint64;
    float flt;
    double dbl;
    bool b;
    std::string str;
};

using DummyPacketPtr = std::shared_ptr<DummyPacket>;
using DummyPacketPtrVec = std::vector<DummyPacketPtr>;

/// Random number/string/struct generators
template <typename T> T generateRandomInt()
{
    constexpr auto minval = std::numeric_limits<T>::min();
    constexpr auto maxval = std::numeric_limits<T>::max();
    static std::uniform_int_distribution<T> distrib(minval, maxval);
    return distrib(gen);
}

template <typename T> T generateRandomFloat()
{
    constexpr auto minval = std::numeric_limits<T>::min();
    constexpr auto maxval = std::numeric_limits<T>::max();
    static std::uniform_real_distribution<T> distrib(minval, maxval);
    return distrib(gen);
}

char generateRandomChar()
{
    return 'A' + rand() % 26;
}

bool generateRandomBool()
{
    return rand() % 2 == 0;
}

std::string generateRandomString(size_t minchars = 2, size_t maxchars = 8)
{
    EXPECT_TRUE(minchars <= maxchars);

    std::string str;
    while (str.size() < minchars)
    {
        str += generateRandomChar();
    }

    while (str.size() < maxchars)
    {
        str += generateRandomChar();
    }

    return str;
}

Colors generateRandomColor()
{
    return static_cast<Colors>(rand() % 6 - 1);
}

DummyPacketPtr generateRandomDummyPacket()
{
    auto s = std::make_shared<DummyPacket>();

    s->e_color = generateRandomColor();
    s->ch = generateRandomChar();
    s->int8 = generateRandomInt<int8_t>();
    s->int16 = generateRandomInt<int16_t>();
    s->int32 = generateRandomInt<int32_t>();
    s->int64 = generateRandomInt<int64_t>();
    s->uint8 = generateRandomInt<uint8_t>();
    s->uint16 = generateRandomInt<uint16_t>();
    s->uint32 = generateRandomInt<uint32_t>();
    s->uint64 = generateRandomInt<uint64_t>();
    s->flt = generateRandomFloat<float>();
    s->dbl = generateRandomFloat<double>();
    s->b = generateRandomBool();
    s->str = generateRandomString();

    return s;
}

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
            //uint64_collectable_->activate(generateRandomInt<uint64_t>());
        } else if (tick == 2000)
        {
            //uint64_collectable_->deactivate();
        }

        // Collect a random bool between ticks 1500 and 2500
        if (tick == 1500)
        {
            //bool_collectable_->activate(rand() % 2 == 0);
        } else if (tick == 2500)
        {
            //bool_collectable_->deactivate();
        }

        // Collect a random enum between ticks 1800 and 2800
        if (tick == 1800)
        {
            //enum_collectable_->activate(generateRandomColor());
        } else if (tick == 2800)
        {
            //enum_collectable_->deactivate();
        }

        // Collect a random DummyPacket between ticks 2000 and 3000
        if (tick == 2000)
        {
            //dummy_packet_collectable_->activate(generateRandomDummyPacket());
        } else if (tick == 3000)
        {
            //dummy_packet_collectable_->deactivate();
        }

        // Collect some different values for just one cycle. To do this, we call
        // the activate() method, passing in "once=true".
        if (tick >= 5000 && tick % 5 == 0)
        {
            //uint64_collectable_->activate(generateRandomInt<uint64_t>(), true);
            //bool_collectable_->activate(rand() % 2 == 0, true);
            //enum_collectable_->activate(generateRandomColor(), true);
            //dummy_packet_collectable_->activate(generateRandomDummyPacket(), true);
        }

        //dummy_collectable_vec_contig_->activate(&dummy_packet_vec_contig_);
        //dummy_collectable_vec_sparse_->activate(&dummy_packet_vec_sparse_);
    }

    uint64_t getCurrentTick() const { return current_tick_; }

private:
    void randomizeDummyPacketCollectables_()
    {
        dummy_packet_vec_contig_.clear();
        for (int i = 0; i < rand() % 10; ++i)
        {
            dummy_packet_vec_contig_.push_back(generateRandomDummyPacket());
        }

        dummy_packet_vec_sparse_.clear();
        dummy_packet_vec_sparse_.resize(32);
        for (int i = 0; i < rand() % 10; ++i)
        {
            if (rand() % 2 == 0)
            {
                dummy_packet_vec_sparse_[i] = generateRandomDummyPacket();
            }
        }
    }

    uint64_t current_tick_ = 0;

    //std::shared_ptr<simdb::CollectionPoint> uint64_collectable_;
    //std::shared_ptr<simdb::CollectionPoint> bool_collectable_;
    //std::shared_ptr<simdb::CollectionPoint> enum_collectable_;
    //std::shared_ptr<simdb::CollectionPoint> dummy_packet_collectable_;

    DummyPacketPtrVec dummy_packet_vec_contig_;
    //std::shared_ptr<simdb::ContigIterableCollectionPoint> dummy_collectable_vec_contig_;

    DummyPacketPtrVec dummy_packet_vec_sparse_;
    //std::shared_ptr<simdb::SparseIterableCollectionPoint> dummy_collectable_vec_sparse_;
};

int main(int argc, char** argv)
{
    (void)argc;(void)argv;
}
