import matplotlib.pyplot as plt

# Data
x_seconds = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]
x = [value * 1000 for value in x_seconds]

y1 = [4422, 4429, 4434, 4438, 4463, 4452, 4443, 4468, 4444]
y2 = [4337, 4251, 4323, 4457, 4294, 4401, 4535, 4368, 4470]

fig, ax = plt.subplots()

ax.plot(x, y1, color='blue', marker='o', markersize=8, label='E2E latency on localhost')
ax.plot(x, y2, color='red', marker='s', markersize=8, label='E2E latency in Docker')

ax.set_ylim(4000, 5000)  # Adjust the y-axis limits
ax.set_xscale('log')

ax.set_xlabel('Lateness Tolerance (ms)')
ax.set_ylabel('End-to-end latency (ms)')
ax.set_title('End-to-end Latencies for Different Lateness Tolerance Limits')

ax.legend()
plt.show()
