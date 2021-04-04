import sys
from pyspark import SparkContext, SparkConf
from pairRdd.aggregation.reducebykey.housePrice.AvgCount import AvgCount

sys.path.insert(0, '.')

if __name__ == "__main__":
    conf = SparkConf().setAppName("avgHousePrice").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("in/RealEstate.csv")
    cleanedLines = lines.filter(lambda line: "Bedrooms" not in line)

    housePricePairRdd = cleanedLines.map(lambda line: \
                                             (line.split(",")[3], AvgCount(1, float(line.split(",")[2]))))

    housePriceTotal = housePricePairRdd \
        .reduceByKey(lambda x, y: AvgCount(x.count + y.count, x.total + y.total))

    print("housePriceTotal: ")
    for bedroom, avgCount in housePriceTotal.collect():
        print("{} : ({}, {})".format(bedroom, avgCount.count, avgCount.total))

    housePriceAvg = housePriceTotal.mapValues(lambda avg_count: avg_count.total / avg_count.count)
    print("\nhousePriceAvg: ")
    for bedroom, avg in housePriceAvg.collect():
        print("{} : {}".format(bedroom, avg))
