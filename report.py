import pandas as pd
import matplotlib.pyplot as plt
import os

# Directory containing the output CSV files
output_dir = "output"  # Update this path as needed

# Define functions for generating insights and graphs
def read_data(file_name):
    file_path = os.path.join(output_dir, file_name)
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        print(f"File {file_name} not found in {output_dir}.")
        return None

def generate_insights_and_graphs():
    # Read CSV files



    minute_level_viewership = read_data("minute_level_viewership.csv")
    total_viewership_hours = read_data("/Users/ritikaramsinghani/Downloads/CaseStudy_DE/mbc_tv_viewership/output/total_viewership_hours/part-00000-cf090ad4-8b12-4ca5-8ecb-3d4a6cf216a2-c000.csv")
    avg_viewing_duration = read_data("/Users/ritikaramsinghani/Downloads/CaseStudy_DE/mbc_tv_viewership/output/average_viewing_duration/part-00000-29789f58-da87-4c09-a640-cbd68a6b9af9-c000.csv")
    top_channels = read_data("/Users/ritikaramsinghani/Downloads/CaseStudy_DE/mbc_tv_viewership/output/top_channels/part-00000-6ee05e77-c2f9-4711-a1e1-a11483e50f20-c000.csv")
    reach_and_trp = read_data("output/reach_and_trp/part-00000-f8037b1f-e816-4d78-b4de-80f54119d456-c000.csv")


    if total_viewership_hours is not None:
        print("\nTotal Viewership Hours:")
        print(total_viewership_hours.head())
        # Plot total viewership hours
        plt.figure(figsize=(10, 6))
        total_viewership_hours.sort_values(by="total_viewership_hours", ascending=False).plot(
            kind="bar", x="Channel", y="total_viewership_hours", color="skyblue"
        )
        plt.title("Total Viewership Hours by Channel", fontsize=14)
        plt.xlabel("Channel", fontsize=12)
        plt.ylabel("Hours", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "total_viewership_hours.png"))
        plt.show()

    if avg_viewing_duration is not None:
        print("\nAverage Viewing Duration Sample:")
        print(avg_viewing_duration.head())
        # Plot average viewing duration
        plt.figure(figsize=(10, 6))
        avg_viewing_duration.sort_values(by="Average Viewing Duration", ascending=False).head(10).plot(
            kind="bar", x="Mac ID", y="Average Viewing Duration", color="orange"
        )
        plt.title("Top 10 Average Viewing Durations", fontsize=14)
        plt.xlabel("Mac ID", fontsize=12)
        plt.ylabel("Minutes", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "average_viewing_duration.png"))
        plt.show()

    if top_channels is not None:
        print("\nTop Channels Sample:")
        print(top_channels.head())
        # Plot top channels
        plt.figure(figsize=(10, 6))
        top_channels.plot(kind="bar", x="Channel Name", y="Total Viewership Hours", color="green")
        plt.title("Top Channels by Total Viewership Hours", fontsize=14)
        plt.xlabel("Channel", fontsize=12)
        plt.ylabel("Total Hours", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "top_channels.png"))
        plt.show()

    if reach_and_trp is not None:
        print("\nReach and TRP Sample:")
        print(reach_and_trp.head())
        # Plot reach and TRP
        plt.figure(figsize=(10, 6))
        reach_and_trp.sort_values(by="Reach", ascending=False).plot(
            kind="bar", x="Channel Name", y="Reach", color="purple"
        )
        plt.title("Reach by Channel", fontsize=14)
        plt.xlabel("Channel", fontsize=12)
        plt.ylabel("Reach", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "reach_by_channel.png"))
        plt.show()

        plt.figure(figsize=(10, 6))
        reach_and_trp.sort_values(by="TRP", ascending=False).plot(
            kind="bar", x="Channel Name", y="TRP", color="red"
        )
        plt.title("TRP by Channel", fontsize=14)
        plt.xlabel("Channel", fontsize=12)
        plt.ylabel("TRP", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "trp_by_channel.png"))
        plt.show()

if __name__ == "__main__":
    generate_insights_and_graphs()
