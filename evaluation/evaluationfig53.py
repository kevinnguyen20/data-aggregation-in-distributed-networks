import matplotlib.pyplot as plt

# Data
x_seconds = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]
x = [value * 1000 for value in x_seconds]

y1 = [24461, 24862, 25659, 23933, 24756, 23694, 23653, 25233, 24491]
y2 = [23207, 22488, 22432, 22240, 21931, 21910, 21598, 22363, 22127]

fig, ax = plt.subplots()

ax.plot(x, y1, color='blue', marker='o', markersize=8, label='Records per second on localhost')
ax.plot(x, y2, color='red', marker='s', markersize=8, label='Records per second in Docker')

ax.set_ylim(0, max(max(y1), max(y2)) + 1000)
ax.set_xscale('log')
ax.set_ylim(15000, 30000)

ax.set_xlabel('Lateness Tolerance (ms)')
ax.set_ylabel('Records per second')
ax.set_title('Records per Second for Different Lateness Tolerance Limits')

ax.legend()
plt.show()
