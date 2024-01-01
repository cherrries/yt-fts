
from .download import get_channel_id_from_input
from .db_utils import * 
from .utils import *
from rich.console import Console


# full text search
def fts_search(text, scope, channel_id=None, video_id=None, limit=None):
    """
    Calls search functions and prints the results 
    """
    console = Console()

    if scope == "all":
        res = search_all(text, limit)
    
    if scope == "channel":
        channel_id = get_channel_id_from_input(channel_id)
        res = search_channel(channel_id, text, limit)
    
    if scope == "video":
        res = search_video(video_id, text, limit)

    if len(res) == 0:
        console.print("- Try shortening the search to specific words")
        console.print("- Try using the wildcard operator [bold]*[/bold] to search for partial words")
        console.print("- Try using the [bold]OR[/bold] operator to search for multiple words")
        if len(text.split(" ")) > 1:
            example_or = text.replace(" ", " OR ")
            console.print(f"    - EX: \"[bold]{example_or}[/bold]\"")
        else: 
            console.print(f"    - EX: \"[bold]foo OR [bold]bar[/bold]\"")
        exit()

    return res


def print_fts_res(res, query):
    console = Console()

    fts_res = []
    channel_names = []

    for channel_id in res:
        console.print(f"Channel ID: {channel_id}")
        for video_id in res[channel_id]['videos']:
            console.print(f"  Video Title: {res[channel_id]['videos'][video_id]['video_title']} | Video ID: {video_id} ")
            for quote in res[channel_id]['videos'][video_id]['results']:
                console.print(f"THiS IS QUOTE: {quote}")
                quote_match = {}
                quote_match["video_id"] = quote["video_id"]
                time_stamp = quote["start_time"]
                time = time_to_secs(time_stamp)
                link = f"https://youtu.be/{quote_match['video_id']}?t={time}"

                quote_match["channel_name"] = get_channel_name_from_video_id(quote_match["video_id"])
                channel_names.append(quote_match["channel_name"])

                quote_match["video_title"] = get_title_from_db(quote_match["video_id"])
                quote_match["subs"] = bold_query_matches(quote["text"].strip(), query)
                quote_match["time_stamp"] = time_stamp
                quote_match["link"] = link 

                fts_res.append(quote_match)

                # Assuming 'context' is a list of dictionaries with 'text' key
                context_before = quote['context_before'] if quote['context_before'] else ''
                context_after = quote['context_after'] if quote['context_after'] else ''
                console.print(f"\tContext Before: {context_before}")
                console.print(f"\tQuote: {quote['text']}")
                console.print(f"\tContext After: {context_after}")
                console.print(f"\tLink: {quote_match['link']}")
                console.print("")

    # sort by channel name
    fts_res = sorted(fts_res, key=lambda x: x["channel_name"])

    num_matches = len(fts_res)
    num_channels = len(set(channel_names))  
    num_videos = len(set([quote["video_id"] for quote in fts_res]))

    summary_str = f"Found [bold]{num_matches}[/bold] matches in [bold]{num_videos}[/bold] videos from [bold]{num_channels}[/bold] channel"

    if num_channels > 1:
        summary_str += "s"

    console.print(summary_str) 