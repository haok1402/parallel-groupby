#include <atomic>
#include <cstdint>
#include <vector>
#include <optional>

#include <zlib.h>
#include <CLI11.hpp>

struct SumAggEntry
{
    std::atomic<int64_t> key;
    std::atomic<int64_t> val;
    SumAggEntry() : key(INT64_MIN), val(0) {}
};

class SumAggMap
{
    public:
        explicit SumAggMap(size_t n)
            : size(n), data(n) {}

        bool upsert(int64_t k, int64_t d)
        {
            size_t i = std::hash<int64_t>{} (k);
            for (size_t probe = 0; probe < size; probe++)
            {
                size_t j = (i + probe) % size;
                int64_t expected = INT64_MIN;

                if (data[j].key.compare_exchange_strong(expected, k))
                {
                    data[j].val.fetch_add(d);
                    return true;
                }
                if (expected == k)
                {
                    data[j].val.fetch_add(d);
                    return true;
                }
            }
            return false;
        }

        std::optional<int64_t> lookup(int64_t k)
        {
            size_t i = std::hash<int64_t>{} (k);
            for (size_t probe = 0; probe < size; probe++)
            {
                size_t j = (i + probe) % size;
                int64_t stored = data[j].key.load();

                if (stored == k)
                {
                    return data[j].val.load();
                }
                if (stored == INT64_MIN)
                {
                    return std::nullopt;
                }
            }
            return std::nullopt;
        }

    private:
        size_t size;
        std::vector<SumAggEntry> data;
};

int main()
{
    SumAggMap map(1024);
    map.upsert(42, 10);
    map.upsert(84, 12);
    map.upsert(42, 24);

    auto val = map.lookup(42);
    if (val) std::cout << "Found 42: " << *val << "\n";
}
