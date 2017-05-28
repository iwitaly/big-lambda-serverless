from api.src.base import Base

from collections import defaultdict
import json


'''
    Basic example here.
    Reducer recieves file in json format. Then transform it to
    dict, accumualte keys and sum freqs.
'''

class Reducer(Base):
    #Global accumulator
    output = defaultdict(int)


    def handler(self, contents):
        '''
        Perform mapping operation over one file with contents
        WRITE HERE YOUR OWN LOGIC
        :param contents: JSON
        :return: self.output
        '''

        for k, v in json.loads(contents).iteritems():
            self.output[k] += v
