import re
import time

# line = " \' hello1 \' somthing \' hello2 \' "
# regex = "\s\'\s"
#
# l = list(line)
# foundOpen = False
# open = 0
# close = 0
# i = 0
# while i < len(l) - 2:
#
#     line = "".join(l) # update the string that matches regex
#     # go through string 3 char at a time to match regex
#     curr = l[i:i+3]
#     if(re.match(regex, line[i:i+3]) != None):
#
#         # havent found open
#         if(not foundOpen):
#             foundOpen = True
#             print(l[i+2:i+3])
#             del l[i+2:i+3]
#
#         # found open
#         elif(foundOpen):
#             foundOpen = False
#             print(l[i:i+1])
#             del l[i:i+1]
#
#     i += 1

# print("".join(l))

def apostrophe(line):
    regex = "\s\'\s"

    l = list(line)
    foundOpen = False
    open = 0
    close = 0
    i = 0
    while i < len(l) - 2:

        line = "".join(l) # update the string that matches regex
        # go through string 3 char at a time to match regex
        curr = l[i:i+3]
        if(re.match(regex, line[i:i+3]) != None):

            # havent found open
            if(not foundOpen):
                foundOpen = True
                # print(l[i+2:i+3])
                del l[i+2:i+3]

            # found open
            elif(foundOpen):
                foundOpen = False
                # print(l[i:i+1])
                del l[i:i+1]

        i += 1

    return "".join(l)

t0 = time.time()
print(apostrophe(" \' hello1 \' somthing \' hello2 \' "))
t1 = time.time()

print(t1 - t0)

# for i in (re.finditer(regex, ("".join(l)))):
    # if(re.match(regex, word) != None):
    #
    #
    # if (isStart):
    #     del l[i.start(0)]
    #     isStart=False
    # else:
    #     del l[i.end(0)-1]
    #     isStart=True
    # print ("".join(l))


# print ("".join(l))
