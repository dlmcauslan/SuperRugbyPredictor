# -*- coding: utf-8 -*-
import re
x = "Try: Stephen R Bachop (2),\nTaine O Randell, Lio Falaniko,\nBrendon Timmins, Kupu Vanisi,\nBrian Lima\nCon: Matthew Cooper (5)\nPen: Matthew Cooper (4)"
#x = "Con: Morné Steyn 17', 32', 45', 49' (4/4)"
#x = "Try: Akona Ndungane  16'\nBjorn Basson  31'\nFrancois Hougaard  44'\nJacques Potgieter  48'\nCon: Morné Steyn 17', 32', 45', 49' (4/4)\nPen: Morné Steyn 14', 38', 65' (3/5)\nCards: Dean Greyling  79'"
x = "Try: Keegan Daniel  44'\nCharl McLeod  58'\nMarcell Coetzee  61'\nRyan Kankowski  74'\nCon: Frédéric Michalak 45', 59', 62', 75' (4/4)\nPen: Frédéric Michalak 14', 31' (2/3)\nFrançois Steyn (0/1)\nDrop: Frédéric Michalak (0/1)\nCards: François Steyn  18' to 30'"
year = 2012
print x
strVect = re.split("[):,\n]",x)
#strVect = re.split("[,]",x)
print strVect

nTries = 0
nCons = 0
nPens = 0
flag = 'N'

for n in strVect:
    if n in ["Try", "Tries"]:
        flag = 'T'
    elif n in ["Con", "Cons", "Cons."]:
        flag = 'C'
    elif n in ["Pen", "Pens", "Pens."]:
        flag = 'P'
    elif n in ["Drop", "Cards"]:
        flag = 'N'
    elif n =='':
        pass
    elif re.match("\s[0-9]{0,2}'$",n):    # Skips times of tries etc, from 2012 onwards
        pass
    elif re.match('\([0-9]{1,2}',re.split(' ',n)[-1]):    # Adds on number of tries etc
        num = int(re.findall('[0-9]{1,2}',re.split(' ',n)[-1])[0])
        if (year >= 2012) & (num!=0):
            num-=1
        print n
        print "{} {}".format(num, flag)    
        if flag == 'T':
            nTries += num 
        elif flag == 'P':
            nPens += num
        elif flag == 'C':
            nCons += num
    else:  
        print n
        print flag
        if flag == 'T':
            nTries += 1
        elif flag == 'P':
            nPens += 1
        elif flag == 'C':
            nCons += 1   
        

print "Tries: {}, Penalties: {}, Conversions: {}".format(nTries, nPens, nCons)  
