#include <gtest/gtest.h>

#include <random>
#include <chrono>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PipelineExecutor.h>


using namespace DB;

using Clock = std::chrono::system_clock;

static Block getBlockWithSize(size_t size_of_row_in_bytes, size_t row_num)
{
    ColumnsWithTypeAndName cols;
    for (size_t i = 0; i < size_of_row_in_bytes; i += sizeof(UInt64))
    {
        auto column = ColumnUInt64::create(row_num, 0);
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), "column" + std::to_string(i));
    }
    return Block(cols);
}

/*
 * repartition one block with total_size_in_bytes into repartition_num smaller blocks.
 */
TEST(Exchange, RepartitionBlock)
{
    size_t total_size_in_bytes = 512000;
    size_t size_of_row_in_bytes = 512;
    size_t total_row_num = total_size_in_bytes / size_of_row_in_bytes;
    size_t repartition_num = 10;

    auto origin_block = getBlockWithSize(size_of_row_in_bytes, total_row_num);

    size_t total_column_num = origin_block.columns();

    std::vector<PaddedPODArray<UInt8>> repartition_filters;
    std::vector<Block> partitioned_blocks;
    partitioned_blocks.reserve(repartition_num);
    repartition_filters.reserve(repartition_num);
    for (int i = 0; i < repartition_num; i++)
    {
        repartition_filters.emplace_back(total_row_num, 0);
        partitioned_blocks.emplace_back(origin_block.cloneEmpty());
    }

    //uniform random init filter
    std::default_random_engine random(time(NULL));
    std::uniform_int_distribution<int> repartition_dist(0, repartition_num - 1);
    for (int i = 0; i < total_row_num; i++)
    {
        int mod_index = repartition_dist(random);
        repartition_filters[mod_index][i] = 1;
    }

    auto start = Clock::now();
    // measure repartition performance
    for (size_t i = 0; i < repartition_num; i++)
    {
        for (size_t j = 0; j < total_column_num; j++)
        {
            partitioned_blocks[i].getByPosition(j).column = origin_block.getByPosition(j).column->filter(repartition_filters[i], 0);
        }
    }
    auto end   = Clock::now();
    std::cout <<  "repartition spend time in microseconds: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;

    size_t total_bytes_after_repartition = 0;
    for(int i = 0; i < repartition_num; i ++){
        total_bytes_after_repartition += partitioned_blocks[i].bytes();
    }
    EXPECT_EQ(total_bytes_after_repartition, total_size_in_bytes);
}


