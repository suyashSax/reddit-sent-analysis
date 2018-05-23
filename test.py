import re

import time

def test(input):
    # input = input.replace(',','.').replace('!','.').replace('?','.').replace(':','.').replace(';','.')
    l=list(input)
    for i in range(len(l)):
        if (l[i]==','):
            if (l[i-1].isdigit() and l[i+1].isdigit()):
                pass
            else:
                l[i]='.'
        if (l[i]=='!' or l[i]=='?' or l[i]==':' or l[i]==';'):
            l[i]='.'

    input="".join(l)
    return input

# line = " \' hello1 \' somthing \' hello2 \' "
# regex = "\s\'\s"
#
# someBool=False
# l = list(line)
#
# matches=[]
#
# for i in (re.finditer(regex, ("".join(l)))):
#     matches.append(i.start(0))
#     matches.append(i.end(0)-1)
#
# del matches[0]
# del matches[len(matches)-1]
#
# i=0
# while (i<len(matches)):
#     if (someBool):
#         del matches[i]
#         i-=1
#         del matches[i+1]
#         i-=1
#         someBool=False
#     else:
#         someBool=True
#     i+=2
#
# l = [i for j, i in enumerate(l) if j not in matches]
#
# print ("".join(l))

print test("hello , ,, ; 60,0!?")
