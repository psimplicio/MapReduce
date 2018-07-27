from mrjob.job import MRJob
import os

class TestJoin(MRJob):
 
    def __init__(self, *args, **kwargs):
        super(TestJoin, self).__init__(*args, **kwargs)
 
    def mapper(self, _, line):

        # Mapper will either get a record from main or join table
        try: # See if it is main table record
            name, product, sale = line.split(',')
            yield name, (product, int(sale), '')
        except ValueError:
            try: # See if it is a join table record
                name, location = line.split(',')
                yield name, ('', '', location)
            except ValueError:
                pass # Record did not match either so skip the record
 
    #Inner Join
    def reducer(self, key, values):
        loc = None
        for product, sale, location in values:
            if location: 
                loc = location
            elif loc: 
                yield key, (product, sale, loc)

    #Left Join
    """
    def reducer(self, key, values):
        loc = None
        for product, sale, location in values:
            if location: loc = location
        yield key, (product, sale, loc)
    """
if __name__ == '__main__':
    TestJoin.run()