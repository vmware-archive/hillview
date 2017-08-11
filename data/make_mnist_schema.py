#!/usr/bin/env python3

if __name__ == "__main__":
	n_pixels = 28**2
	# n_pixels = 5
	print("[", end="")
	print("{", end="")
	print("\"name\": \"label\", ", end="")
	print("\"kind\": \"Integer\", ", end="")
	print("\"allowMissing\": false", end="")
	print("}", end="")
	for i in range(n_pixels):
		print(", {", end="")
		print("\"name\": \"pixel{}\", ".format(i), end="")
		print("\"kind\": \"Integer\", ", end="")
		print("\"allowMissing\": false", end="")
		print("}", end="")
	print("]")
