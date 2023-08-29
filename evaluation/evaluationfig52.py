import matplotlib.pyplot as plt

# Data
x_seconds = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 15, 30, 45, 60]
x = [value * 1000 for value in x_seconds]

y1 = [2467, 16920, 22858, 24673, 25621, 24972, 25417, 24657, 24530, 24172, 22027, 16920, 11533]
y2 = [2767, 17313, 19924, 21193, 21859, 22533, 22358, 22211, 22258, 21591, 19489, 18504, 10128]

fig, ax = plt.subplots()

ax.plot(x, y1, color='blue', marker='o', markersize=8, label='Records per second on localhost')
ax.plot(x, y2, color='red', marker='s', markersize=8, label='Records per second in Docker')

ax.set_ylim(0, max(max(y1), max(y2)) + 1000)  # Adding 1000 for some margin
ax.set_xscale('log')

ax.set_xlabel('Buffer Size (ms)')
ax.set_ylabel('Records per second')
ax.set_title('Records per Second for Different Buffer Sizes')

ax.legend()
plt.show()
