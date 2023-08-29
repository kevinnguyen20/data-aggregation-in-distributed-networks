import matplotlib.pyplot as plt

# Data
x = [18.385, 18.192, 37.503, 25.215, 54.507, 76.147, 102.217, 220.441, 197.011, 169.519, 164.057, 245.143, 243.611, 198.423, 280.197]

y1 = [4396, 4433, 4467, 4448, 4452, 4438, 4445, 4431, 4456, 4487, 4483, 4475, 4494, 4493, 4485, 4446]

y2 = [4282, 4427, 4325, 4357, 4314, 4316, 4337, 4385, 4394, 4374, 4341, 4331, 4406, 4337, 4285, 4377]

data = list(zip(x, y1, y2))

# Sort the data by x values
data.sort(key=lambda entry: entry[0])

x_sorted, y1_sorted, y2_sorted = zip(*data)
fig, ax = plt.subplots()

ax.plot(x_sorted, y1_sorted, color='blue', marker='o', markersize=8, label='Localhost')
ax.plot(x_sorted, y2_sorted, color='red', marker='s', markersize=8, label='Docker')

ax.set_ylim(4000, 5000)

ax.set_xlabel('Delay (ms)')
ax.set_ylabel('End-to-end latency (ms)')
ax.set_title('End-to-end latency for specific delays')

ax.legend()
plt.show()
