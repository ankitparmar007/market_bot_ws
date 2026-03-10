# #!/bin/bash

# # Create a new tmux session called 'prod' detached (-d)
# tmux new-session -d -s marketbot

# # Run 4 processes in separate tmux windows (or panes)
# tmux send-keys -t marketbot "cd /root/Desktop/market_bot_ws && bash run.sh" C-m