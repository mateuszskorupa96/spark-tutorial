from pyspark import SparkContext, SparkConf

from commons.Utils import Utils

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count")
    sc = SparkContext(conf=conf)
    # create new RDD
    inputIntegers = list(range(1, 6))
    integerRdd = sc.parallelize(inputIntegers)

    sums = integerRdd.reduce(lambda a, b: a + b)
    print(sums)

    def split_comma(line: str):
        splits = Utils.COMMA_DELIMITER.split(line)
        return "{}, {}".format(splits[1], splits[2])


    def split_comma_latitude(line: str):
        splits = Utils.COMMA_DELIMITER.split(line)
        return "{}, {}".format(splits[1], splits[6])


    def starts_with_header(line: str):
        return not line.startswith("host")

    airports = sc.textFile("/home/osboxes/Downloads/CODES/python-spark-tutorial-master/in/airports.text")
    # airports_in_usa = airports\
    #     .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[3] == "\"United States\"")
    # airports_name_and_city = airports_in_usa.map(split_comma)
    # airports_name_and_city.saveAsTextFile("out/airports_in_usa.text")

    # airports_by_latitude = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)
    # airports_name_and_latitude = airports_by_latitude.map(split_comma_latitude)
    # airports_name_and_latitude.saveAsTextFile("out/airports_with_latitude_greater_than_40")

    julyLogs = sc.textFile("/home/osboxes/Downloads/CODES/python-spark-tutorial-master/in/nasa_19950701.tsv")
    augustLogs = sc.textFile("/home/osboxes/Downloads/CODES/python-spark-tutorial-master/in/nasa_19950801.tsv")

    # aggregatedLogs = julyLogs.union(augustLogs)
    # aggregatedLogsWithoutHeader = aggregatedLogs.filter(starts_with_header)
    #
    # sampleLogs = aggregatedLogsWithoutHeader.sample(withReplacement=True, fraction=0.1)
    #
    # sampleLogs.saveAsTextFile("out/logs_union_problem")

    # firstElementsJulyLogs = julyLogs.map(lambda line: str(line).split("\t")[0])
    # firstElementsAugustLogs = augustLogs.map(lambda line: str(line).split("\t")[0])
    #
    # intersectedHosts = firstElementsJulyLogs.intersection(firstElementsAugustLogs).filter(starts_with_header)
    #
    # intersectedHosts.saveAsTextFile("out/intersection_problem")

    # inputIntegers = [1, 2, 3, 4, 5, 6]
    #
    # integerRdd = sc.parallelize(inputIntegers)
    # result = integerRdd.reduce(lambda a, b: a*b)
    # print(result)

    primeNumbers = sc.textFile("/home/osboxes/Downloads/CODES/python-spark-tutorial-master/in/prime_nums.text")

    primeNumbers = primeNumbers.flatMap(lambda number: number.split("\t"))
    splitNumbers = primeNumbers.filter(lambda number: number)

    print(splitNumbers.take(10))

    splitNumbers = splitNumbers.map(lambda number: int(number)).take(100)

    print(sc.parallelize(splitNumbers).reduce(lambda a, b: a + b))







