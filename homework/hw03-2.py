from mrjob.job import MRJob

class monthreviewcnt(MRJob):

    def mapper(self, _, line):
        row = line.split(',')
        date = row[1]
        date_month = date[0:7]
        if date != 'date':
            yield date_month,1

    def reducer(self, key, values):
        yield key,sum(values)


if __name__ == '__main__':
    monthreviewcnt()


mr_job = monthreviewcnt(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)
