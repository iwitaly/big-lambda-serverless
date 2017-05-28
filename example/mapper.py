from api.src.base import Base

from collections import defaultdict


'''
    Basic example here.
    Mapper just read lines from context, split line by comma and
    calculate number of accurance for each first element in splitted array
'''
class Mapper(Base):
    #Global accumulator
    output = defaultdict(int)


    def handler(self, contents):
        '''
        Perform mapping operation over one file with contents
        WRITE HERE YOUR OWN LOGIC
        :param contents: unparsed CSV file
        :return: self.output
        '''

        for line in contents.split('\n')[:-1]:
            # line_count += 1
            data = line.split(',')
            k = data[0]
            self.output[k] += 1
