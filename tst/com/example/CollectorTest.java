package com.example;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by Matcha on 1/3/16.
 */
public class CollectorTest {
    @Data
    @AllArgsConstructor
    private class Bucket {
        String name;
        Integer value;

        public Bucket emptyBucket() {
            return new Bucket(name, 0);
        }
    }

    /**
     * Aggregates buckets on their names. Buckets with the same name has their value aggregated.
     */
    private class BucketCollector implements Collector<Bucket, Map<String, Bucket>, List<Bucket>> {
        @Override
        public Supplier<Map<String, Bucket>> supplier() {
            return () -> new HashMap<>();
        }

        @Override
        public BiConsumer<Map<String, Bucket>, Bucket> accumulator() {
            return (stringBucketMap, bucket) ->
                stringBucketMap.merge(bucket.getName(), bucket, (bucket1, bucket2) -> {
                    bucket1.setValue(bucket1.getValue() + bucket2.getValue());
                    return bucket1;
                });
        }

        @Override
        public BinaryOperator<Map<String, Bucket>> combiner() {
            return null;
        }

        @Override
        public Function<Map<String, Bucket>, List<Bucket>> finisher() {
            System.out.println("Finisher");
            return stringBucketMap -> stringBucketMap.values().stream().collect(Collectors.toList());
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.UNORDERED));
        }
    }


    @Test
    public void bucketCollectorTest() {

        List<Bucket> bucketList = new ArrayList<>();
        bucketList.add(new Bucket("A", 1));
        bucketList.add(new Bucket("B", 2));
        bucketList.add(new Bucket("A", 3));
        bucketList.add(new Bucket("C", 4));

        List<Bucket> aggregatedBucketList = bucketList.stream().collect(new BucketCollector());

        Comparator<Bucket> bucketComparator = (o1, o2) -> StringUtils.getLevenshteinDistance(o1.getName(), o2.getName());

        aggregatedBucketList.sort(bucketComparator);


        Assert.assertEquals(aggregatedBucketList, ImmutableList.of(new Bucket("A", 4), new Bucket("B", 2), new Bucket("C", 4)));
    }
}
