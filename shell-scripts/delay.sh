mean=100
stddev=25
min_delay=250
max_delay=450
delaying=false

create_ifb_interface() {
  sudo ip link add dev ifb0 type ifb
  sudo ip link set dev ifb0 up
}

start_delay() {
  if $delaying; then
    echo "Delaying is already in progress."
    return
  fi

  create_ifb_interface

  delaying=true

  while $delaying; do
    delay=$(shuf -i ${min_delay}-${max_delay} -n 1)

    # Remove existing configurations if present
    sudo tc qdisc del dev ifb0 root
    sudo tc qdisc del dev lo ingress

    # Set delay on ifb0 and redirect loopback traffic
    sudo tc qdisc add dev ifb0 root netem delay ${delay}ms
    sudo tc qdisc add dev lo ingress
    sudo tc filter add dev lo parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb0

    sleep 1
  done
}

stop_delay() {
  if ! $delaying; then
    echo "Delaying is not in progress."
    return
  fi

  delaying=false

  sudo tc qdisc del dev ifb0 root netem
  sudo tc qdisc del dev lo ingress
  sudo ip link del dev ifb0
}

case "$1" in
  "start")
    start_delay
    ;;
  "stop")
    stop_delay
    ;;
  *)
    echo "Usage: $0 [start|stop]"
    exit 1
    ;;
esac
