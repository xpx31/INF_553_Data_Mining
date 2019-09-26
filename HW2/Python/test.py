f_list = list([('0x61', '0x63'), ('0x61', '0x64'), ('0x61', '0x65')])

print(f_list)


def getCandidateItemMore(frequentItems, lengthOfPair):
	subsetOfSizeMore = list()
	lengthA = len(frequentItems) - 1
	lengthB = lengthA + 1
	for i in range(lengthA):
		for j in range(i + 1, lengthB):
			partA = frequentItems[i]
			partB = frequentItems[j]
			if partA[0:(lengthOfPair - 2)] == partB[0:(lengthOfPair - 2)]:
				subsetOfSizeMore.append(tuple(sorted(set(partA).union(partB))))
			else:
				break

	return subsetOfSizeMore

print(getCandidateItemMore(f_list, 3))

print(tuple(sorted(('abc', 'bde', 'aac'))))
