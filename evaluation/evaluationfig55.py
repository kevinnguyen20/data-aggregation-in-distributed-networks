import matplotlib.pyplot as plt

# Data
x_seconds = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 15, 30, 45, 60]
x_milliseconds = [value * 1000 for value in x_seconds]

y1 = [2461, 11791, 17726, 1715, 1735, 1838, 1978, 3039, 4457, 5830, 9788, 11729, 16766]
y2 = [2917, 12062, 16775, 1720, 1716, 1839, 1933, 3002, 4315, 5775, 9302, 11151, 13907]

fig, ax = plt.subplots()

ax.plot(x_milliseconds, y1, color='blue', marker='o', markersize=8, label='E2E latency on localhost')
ax.plot(x_milliseconds, y2, color='red', marker='s', markersize=8, label='E2E latency in Docker')

ax.set_ylim(0, max(max(y1), max(y2)) + 1000)
ax.set_xscale('log')

ax.set_xlabel('Buffer Size (ms)')
ax.set_ylabel('End-to-end latency (ms)')
ax.set_title('End-to-end Latencies for Different Buffer Sizes')

ax.legend()
plt.show()
