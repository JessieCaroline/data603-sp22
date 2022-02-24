from mrjob.job import MRJob

class avgratecool(MRJob):

    def mapper(self, _, line):
        row = line.split(',')
        rating = row[3]
        cool = row[7]
        if cool != 0 and rating != 'stars':
            yield "coolrating", int(rating)

    def reducer(self, key, values):
        ratinglist = list(values)
        yield "Average rating for cool !=0 : ", (sum(ratinglist) / len(ratinglist))


if __name__ == '__main__':
    avgratecool()


mr_job = avgratecool(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)
