import matplotlib.pyplot as plt

# Data
x = [18.385, 18.192, 37.503, 25.215, 54.507, 76.147, 102.217, 220.441, 197.011, 169.519, 164.057, 245.143, 243.611, 198.423, 280.197]

y1 = [24329, 24563, 24616, 24656, 24516, 24890, 24580, 24386, 24485, 24576, 24595, 24109, 25171, 25476, 25253, 24633]

y2 = [23044, 22328, 22765, 22471, 22871, 22313, 21999, 22536, 22909, 23079, 23264, 22178, 22039, 22033, 22072, 22765]

data = list(zip(x, y1, y2))

# Sort the data by x values
data.sort(key=lambda entry: entry[0])

x_sorted, y1_sorted, y2_sorted = zip(*data)
fig, ax = plt.subplots()

ax.plot(x_sorted, y1_sorted, color='blue', marker='o', markersize=8, label='Records per second on localhost')
ax.plot(x_sorted, y2_sorted, color='red', marker='s', markersize=8, label='Records per second in Docker')

ax.set_ylim(15000, 30000)

ax.set_xlabel('Delay (ms)')
ax.set_ylabel('Records per second')
ax.set_title('Records Per Second for Specific Delays')

ax.legend()
plt.show()
