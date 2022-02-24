from mrjob.job import MRJob

class avgreview(MRJob):

    def mapper(self, _, line):
        row = line.split(',')
        words = row[4]
        wrdcnt = len(words.split())
        if words != 'text':
            yield "wordcount", wrdcnt

    def reducer(self, key, values):
        wrdcntlist = list(values)
        yield "Average word count in each review : ", (sum(wrdcntlist) / len(wrdcntlist))


if __name__ == '__main__':
    avgreview()


mr_job = avgreview(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)
