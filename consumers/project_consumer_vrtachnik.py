"""
project_consumer_vrtachnik.py

Read a JSON-formatted file as it is being written and visualize
average sentiment by author in real time.
"""

#####################################
# Import Modules
#####################################

import json
import os
import sys
import time
import pathlib
from collections import defaultdict

import matplotlib.pyplot as plt
from utils.utils_logger import logger


#####################################
# Set up Paths - read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
# Producer writes here
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

# Track message count and sentiment sum per author
author_stats = defaultdict(lambda: {"count": 0, "sentiment_sum": 0.0})

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots()
plt.ion()  # interactive mode for live updates

#####################################
# Update Chart Function
#####################################

def update_chart():
    """Update the live bar chart with average sentiment by author."""
    ax.clear()

    authors = list(author_stats.keys())
    counts = [author_stats[a]["count"] for a in authors]
    avgs = [
        (author_stats[a]["sentiment_sum"] / author_stats[a]["count"])
        if author_stats[a]["count"] else 0.5
        for a in authors
    ]

    bars = ax.bar(authors, avgs, color="skyblue")

    # annotate each bar with the number of messages
    for rect, c in zip(bars, counts):
        ax.annotate(f"n={c}",
                    (rect.get_x() + rect.get_width()/2, rect.get_height()),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", va="bottom", fontsize=8)

    ax.set_ylim(0, 1)
    ax.set_xlabel("Authors")
    ax.set_ylabel("Average Sentiment (0–1)")
    ax.set_title("Average Sentiment by Author (live)")
    ax.set_xticklabels(authors, rotation=45, ha="right")
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Process Message Function
#####################################

def process_message(message: str) -> None:
    """Process one JSON message and update chart."""
    try:
        message_dict = json.loads(message)
        if isinstance(message_dict, dict):
            author = message_dict.get("author", "unknown")
            sentiment = float(message_dict.get("sentiment", 0.5))

            # update stats
            stats = author_stats[author]
            stats["count"] += 1
            stats["sentiment_sum"] += sentiment

            logger.info(f"Processed message from {author}, sentiment={sentiment}")
            update_chart()
        else:
            logger.error(f"Expected dict, got {type(message_dict)}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Function
#####################################

def main():
    logger.info("START consumer.")

    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        with open(DATA_FILE, "r") as file:
            file.seek(0, os.SEEK_END)  # start at end
            print("Consumer ready. Waiting for new messages...")

            while True:
                line = file.readline()
                if line.strip():
                    process_message(line)
                else:
                    time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")

#####################################
# Run if executed directly
#####################################

if __name__ == "__main__":
    main()
