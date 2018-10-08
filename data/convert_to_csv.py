#!/usr/bin/env python3
# convert adapted from: http://pjreddie.com/projects/mnist-in-csv/
# pylint: disable=invalid-name,missing-docstring

def convert(imgf, labelf, outf, n):
    f = open(imgf, "rb")
    o = open(outf, "w")
    label = open(labelf, "rb")

    f.read(16)
    label.read(8)
    images = []

    for i in range(n):
        image = [ord(label.read(1))]
        for j in range(28 * 28):
            image.append(ord(f.read(1)))
        images.append(image)

    o.write(",".join(["label"] + ["pixel{}".format(i) for i in range(28 * 28)]) + "\n")
    for image in images:
        o.write(",".join(str(pix) for pix in image) + "\n")
    f.close()
    o.close()
    label.close()


if __name__ == "__main__":
    # Convert the data
    convert("train-images-idx3-ubyte", "train-labels-idx1-ubyte",
            "mnist_train.csv", 60000)
    convert("t10k-images-idx3-ubyte", "t10k-labels-idx1-ubyte",
            "mnist_test.csv", 10000)

    # Generate the schema file.
    n_pixels = 28**2
    with open('mnist.schema', 'w') as f:
        column = "{\"name\": \"label\", \"kind\": \"String\", \"allowMissing\": false}"
        print("[" + column, end="", file=f)
        for i in range(n_pixels):
            column = "{\"name\": \"pixel%d\", \"kind\": \"Integer\", \"allowMissing\": false}" % i
            print(", " + column, end="", file=f)
        print("]", file=f)
