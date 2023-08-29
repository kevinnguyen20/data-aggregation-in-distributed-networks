import matplotlib.pyplot as plt

# Data
x_seconds = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 15, 30, 45, 60]
x = [value * 1000 for value in x_seconds]

backpressured_local = [99, 96, 91, 0, 0, 0, 0, 0, 0, 2, 2, 3, 4]
backpressured_docker = [99, 96, 90, 0, 0, 0, 0, 1, 3, 4, 4, 7, 8]

fig, ax = plt.subplots()

ax.plot(x, backpressured_local, color='blue', marker='o', markersize=8, label='Backpressured on localhost')
ax.plot(x, backpressured_docker, color='red', marker='s', markersize=8, label='Backpressured in Docker')

ax.set_xscale('log')
ax.set_xlim(min(x), max(x))
ax.set_ylim(0, 100)

ax.set_xlabel('Buffer Size (ms)')
ax.set_ylabel('Backpressure Rate (%)')
ax.set_title('Backpressure Rates for Different Buffer Sizes')

ax.legend()
plt.show()
