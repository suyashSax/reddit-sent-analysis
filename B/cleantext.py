#!/usr/bin/env python

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse

from string import punctuation
import sys
import json
import os

__author__ = ""
__email__ = ""

# Some useful data.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    # YOUR CODE GOES BELOW:

    # 1. Replace new lines and tab characters with a single space.
    text = replaceBreaks(text)
    #print("------------------")
    #print(text)

    # 2. Remove URLs
    text = splitURLs(text)
    #print("/////////////")
    #print(text)
    # print(text)

    # 3. Split text on a single space.
    # If there are multiple contiguous spaces, you will need to remove empty tokens after doing the split.
    text = splitSpace(text)
    #print("/////////////")
    #print(text)
    # print(text)

    # 4. Separate all external punctuation such as periods, commas, etc. into their own tokens
    text = stepFour(text)
    # print("/////////////")
    # print(text)

    # 5
    text = stepFive(text)
    # print("/////////////")
    # print(text)

    # 6. Convert all text to lowercase
    text = text.lower()

    # remove extraneous white space
    text = re.sub("\\s+"," ", text)

    text = text.strip()

    text = apostrophe(text)

    unigrams = ngrams(text, 1)
    bigrams = ngrams(text, 2)
    trigrams = ngrams(text, 3)
    # print(text)

    return [text, unigrams, bigrams, trigrams]

def replaceBreaks(s):
    s.replace('\n', ' ')
    s.replace('\t', ' ')
    return s

def splitURLs(s):
    return re.sub(r'http\S+', '', s)

def splitSpace(s):
    s = s.split()
    s = filter(None, s)
    s = ' '.join(s)
    return s

# from string import punctuation
def stepFour(s):
    s = s.split()

    modify_ends = s[:]
    result_strings = s[:]

    for word in range(len(s)):
        if(s[word][0] in punctuation and s[word][-1] in punctuation):
            if(s[word][0] == '\'' and s[word][-1] == '\''):
                modify_ends[word] = s[word]
            elif((s[word][0] == '$' or s[word][0] == '%') and (s[word][-1] == "$" or s[word][-1]== '%')):
                modify_ends[word] = s[word]
            elif((s[word][0] == '$' or s[word][0] == '%')):
                modify_ends[word] = s[word][0:-1] + " " + s[word][-1]
            elif((s[word][-1] == '$' or s[word][-1] == '%')):
                modify_ends[word] = s[word][0] + " " + s[word][1:-1]
            else:
                modify_ends[word] = s[word][0] + " " + s[word][1:-1] + " " + s[word][-1]

        elif(s[word][-1] in punctuation):
            #print(s[word])
            if(s[word][-1] == '$' or s[word][-1] == '%'):
                #print(s[word])
                modify_ends[word] = s[word]
            else:
                modify_ends[word] = s[word][:-1] + " " + s[word][-1]

        elif(s[word][0] in punctuation):
            #print(s[word])
            if(s[word] == "'ol" or s[word] == "'tis'"):
                result_strings[word] = modify_ends[word]
            elif(s[word][0] == '$' or s[word][0] == '%'):
                #print(modify_ends[word])
                result_strings[word] = modify_ends[word]
            else:
                modify_ends[word] = s[word][0] + " " + s[word][1:]
            #print final_strings[word]

        else:
            result_strings[word] = modify_ends[word]
            continue

        #print(final_strings)

        result_strings[word] = modify_ends[word][0]

        #print(final_strings[word])

        for char in range(1, len(modify_ends[word])-1):
            if(modify_ends[word][char] in punctuation):
                if((modify_ends[word][char-1].isalpha() or modify_ends[word][char-1].isdigit()) and (modify_ends[word][char+1].isalpha() or modify_ends[word][char-1].isdigit())):
                    result_strings[word] += modify_ends[word][char]
                elif (modify_ends[word][char-1].isalpha() or modify_ends[word][char-1].isdigit()):
                    if(modify_ends[word][char] == "%" or modify_ends[word][char] == "$"):
                        result_strings[word] += modify_ends[word][char]
                    else:
                        result_strings[word] += " " + modify_ends[word][char]
                    #print(result_strings[word])

                elif (modify_ends[word][char+1].isalpha() or modify_ends[word][char+1].isdigit()):
                    if(modify_ends[word][char] == "%" or modify_ends[word][char] == "$"):
                        result_strings[word] += modify_ends[word][char]
                    else:
                        result_strings[word] += modify_ends[word][char]+ " "

                else:
                    result_strings[word] += " " + modify_ends[word][char]

            else:
                result_strings[word] += modify_ends[word][char]

        if(len(s[word]) > 1):
            result_strings[word] += modify_ends[word][-1]

    s = ' '.join(result_strings)
    s = re.sub("\\s+"," ", s)
    s = s.strip()
    return s

def stepFive(s):
    s = s.lower()
    # keep list of words to remove from sentence
    remove = []

    # if character isn't in the list below
    regex = "[^a-z0-9.?!,;:$%' ]+"

    # split sentence into tokens
    spl = s.split()
    # print("****")

    for word in spl:
        if(re.match(regex, word) != None):

            # matches internal punctuation
            reg_between = "[a-z0-9]+[^a-z0-9][a-z0-9]+"
            # print(word)
            if(re.match(reg_between, word) == None):
                remove.append(word)

    for word in remove:
        spl.remove(word)

    result = " ".join(spl)
    # print(result)
    # print("****")

    # v = re.sub(regex," ",s)
    return result

def ngrams(input, n):
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

    phrases=input.split('.')
    i=0
    while(i<len(phrases)):
        phrases[i]=phrases[i].strip()
        phrases[i]=phrases[i].split()
        if (len(phrases[i])<n):
            del phrases[i]
            i-=1
        i+=1
    output = []
    for p in phrases:
        for i in range(len(p)-n+1):
            output.append(p[i:i+n])
    for i in range(len(output)):
        output[i]='_'.join(output[i])
    return ' '.join(output)

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

def main():

    if(len(sys.argv) != 2):
        print("pass one file to the script")
        sys.exit()

    filename = sys.argv[1]

    extension = os.path.splitext(filename)[1]

    try:
        if extension == '.bz2':
            file = bz2.BZ2File(filename, 'rb', 1000000)
        else:
            file = open(filename, "r")
    except OSError:
        print("can't open file")

    data = []

    # UNCOMMENT
    for line in file:
        # print(line)
        parsed = json.loads(line)
        s = sanitize(parsed['body'])
        data.append(s)

    for line in data:
        print(line)

if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.

    main()
