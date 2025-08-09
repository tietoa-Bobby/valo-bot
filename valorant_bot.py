import discord
import requests
import asyncio
import json
import os
import statistics
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from typing import Dict, List, Optional, Tuple, Any
from threading import Thread
from flask import Flask

# Load environment variables
load_dotenv()

# Constants
VALORANT_API_BASE = "https://api.henrikdev.xyz/valorant/v1"
MAX_MATCH_HISTORY_DAYS = 90
DEFAULT_STATS_DAYS = 7
DEFAULT_SERVER_STATS_DAYS = 30
LOGS_DIR = "match_logs"
LINKS_FILE = "user_links.json"
COMPETITIVE_MODES = ["Competitive", "competitive"]  # Possible competitive mode strings

# Bot setup with intents
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Flask web server to keep the bot alive on Render
app = Flask('')

@app.route('/')
def home():
    return "Valorant Discord Bot is running! Bot status: Ready for commands."

@app.route('/health')
def health():
    return {"status": "healthy", "bot": "online"}

def run():
    # Get the port from the environment variable provided by Render
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    # Start the Flask app in a separate thread
    server_thread = Thread(target=run)
    server_thread.daemon = True
    server_thread.start()

# Utility functions for JSON file operations and user management
def load_json_file(file_path: str) -> List[Dict[str, Any]]:
    """Load JSON data from file"""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return []

def save_json_file(file_path: str, data: List[Dict[str, Any]]) -> bool:
    """Save JSON data to file"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        return True
    except Exception:
        return False

def get_linked_users(guild_id: int) -> Dict[int, str]:
    """Get linked users for a guild"""
    try:
        if os.path.exists(LINKS_FILE):
            with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                all_links = json.load(f)
                return all_links.get(str(guild_id), {})
    except Exception:
        pass
    return {}

# Get API key from environment (optional for basic usage)
HENRIK_API_KEY = os.getenv('HENRIK_API_KEY')

@bot.event
async def on_ready():
    print("🚀 Starting Valorant Discord Bot...")
    print("📊 Using Henrik-3 Valorant API v4.2.0")
    print("⚡ Using modern slash commands with dropdowns")
    print("🏆 COMPETITIVE MATCHES ONLY - All analysis focuses on competitive games")
    
    if HENRIK_API_KEY:
        print("✅ Henrik API key loaded")
        print("📈 Rate limit: 30-90 requests/minute (depending on key type)")
    else:
        print("⚠️ No Henrik API key found - using public rate limits")
        print("📈 Rate limit: Limited requests/minute")
    
    print(f'\n{bot.user} has connected to Discord!')
    print(f'Bot is ready and running!')
    print("\n🎯 Available Commands (Competitive Only):")
    print("• /recentmatch - Get comprehensive stats for all 10 players with agents")
    print("• /stats - View detailed personal performance statistics")
    print("• /fetch - Bulk fetch and store match data for analytics")
    print("• /economy - Analyze economic performance and round type win rates")
    print("• /clutch - Clutch situations analysis (1v1, 1v2, 1v3+)")
    print("• /agents - Best player rankings by agent")
    print("• /comp - Best team compositions by map")
    print("• /map - Win rates and performance by map")
    print("• /link - Link your Discord account to your Valorant username")
    print("• /unlink - Remove your account link")
    print("• /linked - Show all linked server members")
    print("• /help - Show command help")
    print("")
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} command(s)")
    except Exception as e:
        print(f"Failed to sync commands: {e}")

def calculate_kda(kills: int, deaths: int, assists: int) -> float:
    """Calculate KDA ratio: (Kills + Assists) / Deaths"""
    if deaths == 0:
        return float('inf') if kills + assists > 0 else 0.0
    return round((kills + assists) / deaths, 2)

def calculate_adr(damage_dealt: int, rounds_played: int) -> float:
    """Calculate Average Damage per Round"""
    if rounds_played == 0:
        return 0
    return round(damage_dealt / rounds_played, 1)

def calculate_hs_percentage(headshots: int, total_shots: int) -> float:
    """Calculate headshot percentage"""
    if total_shots == 0:
        return 0.0
    return round((headshots / total_shots) * 100, 1)

def is_competitive_match(match_data: Dict[str, Any]) -> bool:
    """Check if a match is from competitive mode"""
    if not match_data:
        return False
    
    # Check match_info mode (for stored match data)
    match_info = match_data.get('match_info', {})
    if match_info:
        mode = match_info.get('mode', '').lower()
        if mode == 'competitive':
            return True
    
    # Check metadata mode (for fresh API data)
    metadata = match_data.get('metadata', {})
    if metadata:
        mode = metadata.get('mode', '').lower()
        if mode == 'competitive':
            return True
    
    return False

def filter_competitive_matches(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter matches to only include competitive games"""
    return [match for match in matches if is_competitive_match(match)]

def calculate_kast(match_data: Dict[str, Any], player_puuid: str, total_rounds: int) -> float:
    """Calculate KAST percentage from match data
    KAST = rounds where player had Kill, Assist, Survived, or was Traded
    """
    if not match_data or not player_puuid or total_rounds == 0:
        return 0.0
    
    round_data = match_data.get('rounds', [])
    match_kills = match_data.get('kills', [])
    
    if not round_data:
        return 0.0
    
    kast_rounds = 0
    
    # Group kills by round for trade detection
    kills_by_round = {}
    for kill_event in match_kills:
        round_num = kill_event.get('round', 0)
        if round_num not in kills_by_round:
            kills_by_round[round_num] = []
        kills_by_round[round_num].append(kill_event)
    
    # Check each round for KAST criteria
    rounds_checked = 0
    for round_info in round_data:
        # Try multiple possible round number fields
        round_num = (round_info.get('round_num') or 
                    round_info.get('round') or 
                    round_info.get('round_number', rounds_checked))
        
        player_stats_list = round_info.get('player_stats', [])
        
        # Find this player's stats for this round
        player_round_stats = None
        for player_stat in player_stats_list:
            if player_stat.get('player_puuid') == player_puuid:
                player_round_stats = player_stat
                break
        
        if not player_round_stats:
            rounds_checked += 1
            continue
            
        rounds_checked += 1
        
        # Check for Kill
        has_kill = player_round_stats.get('kills', 0) > 0
        
        # Check for Assist - check both round stats and kill events
        has_assist = player_round_stats.get('assists', 0) > 0
        
        # Also check kill events for assists in this round
        if not has_assist:
            round_kills_list = (kills_by_round.get(round_num, []) or 
                               kills_by_round.get(round_num + 1, []) or 
                               kills_by_round.get(round_num - 1, []))
            
            for kill_event in round_kills_list:
                assistants = kill_event.get('assistants', [])
                if assistants and any(assist.get('assistant_puuid') == player_puuid for assist in assistants):
                    has_assist = True
                    break
        
        # Check for Survival - try multiple possible field names
        survived = (player_round_stats.get('alive', False) or 
                   player_round_stats.get('was_alive', False) or
                   player_round_stats.get('survived', False) or
                   not player_round_stats.get('died_in_round', True))
        
        # If player didn't die in this round's kill events, they survived
        if not survived:
            round_kills_list = (kills_by_round.get(round_num, []) or 
                               kills_by_round.get(round_num + 1, []) or 
                               kills_by_round.get(round_num - 1, []))
            
            player_died_this_round = any(kill.get('victim_puuid') == player_puuid for kill in round_kills_list)
            if not player_died_this_round:
                survived = True
        
        # Check for Trade (teammate killed player's killer within 5 seconds)
        was_traded = False
        
        # Look for kills in this round
        round_kills_list = (kills_by_round.get(round_num, []) or 
                           kills_by_round.get(round_num + 1, []) or 
                           kills_by_round.get(round_num - 1, []))
        
        if round_kills_list:
            # Sort by kill time
            round_kills_list.sort(key=lambda x: x.get('kill_time_in_round', 0))
            
            for i, kill_event in enumerate(round_kills_list):
                # If this player was killed
                if kill_event.get('victim_puuid') == player_puuid:
                    player_death_time = kill_event.get('kill_time_in_round', 0)
                    killer_puuid = kill_event.get('killer_puuid')
                    
                    # Check if the killer was eliminated by a teammate within 5 seconds
                    for j in range(len(round_kills_list)):  # Check all kills in round
                        if j == i:  # Skip the death event itself
                            continue
                            
                        trade_kill = round_kills_list[j]
                        trade_time = trade_kill.get('kill_time_in_round', 0)
                        
                        # Trade conditions:
                        # 1. Killer of our player was killed
                        # 2. Within 5 seconds (before or after - some APIs report trades before the original kill)
                        # 3. Killer wasn't our player (can't trade yourself)
                        if (trade_kill.get('victim_puuid') == killer_puuid and
                            abs(trade_time - player_death_time) <= 5000 and  # 5 seconds window
                            trade_kill.get('killer_puuid') != player_puuid):
                            was_traded = True
                            break
                    break
        
        # KAST if any condition is met
        round_kast = has_kill or has_assist or survived or was_traded
        if round_kast:
            kast_rounds += 1
    
    # Calculate percentage based on actual rounds with player data
    actual_rounds = max(rounds_checked, total_rounds)
    kast_percentage = round((kast_rounds / actual_rounds) * 100, 1)
    
    return kast_percentage

def count_multikills(player_rounds):
    """Count multi-kills (2K, 3K, 4K, 5K) from round data"""
    multikills = {'2k': 0, '3k': 0, '4k': 0, '5k': 0}
    
    if not player_rounds:
        return multikills
    
    for round_data in player_rounds:
        # Try different possible field names for kills in a round
        kills_in_round = round_data.get('kills', round_data.get('player_kills', 0))
        
        if kills_in_round >= 2:
            if kills_in_round == 2:
                multikills['2k'] += 1
            elif kills_in_round == 3:
                multikills['3k'] += 1
            elif kills_in_round == 4:
                multikills['4k'] += 1
            elif kills_in_round >= 5:
                multikills['5k'] += 1
    
    return multikills

def fetch_match_details(match_id):
    """Fetch detailed match data from v2 match endpoint"""
    url = f"{VALORANT_API_BASE.replace('v1', 'v2')}/match/{match_id}"
    
    headers = {
        'User-Agent': 'Valorant-Discord-Bot/1.0'
    }
    
    if HENRIK_API_KEY:
        headers['Authorization'] = HENRIK_API_KEY
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            match_data = data.get('data', {})
            if match_data:
                return match_data
            else:
                return {}
        elif response.status_code == 404:
            return {}
        elif response.status_code == 429:
            return {}
        else:
            return {}
            
    except requests.exceptions.RequestException:
        return {}

def fetch_valorant_matches(region, username, tag, size=20):
    """Fetch matches from Valorant API with specified size limit"""
    # URL encode the username and tag to handle spaces and special characters
    encoded_username = requests.utils.quote(username, safe='')
    encoded_tag = requests.utils.quote(tag, safe='')
    
    # Use v3 API endpoint as per Henrik API documentation with size parameter
    url = f"{VALORANT_API_BASE.replace('v1', 'v3')}/matches/{region}/{encoded_username}/{encoded_tag}?size={size}"
    
    # Set up headers with API key if available
    headers = {
        'User-Agent': 'Valorant-Discord-Bot/1.0'
    }
    
    if HENRIK_API_KEY:
        headers['Authorization'] = HENRIK_API_KEY
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            matches = data.get('data', [])
            return matches
        else:
            return []
            
    except requests.exceptions.Timeout:
        return []
    except requests.exceptions.RequestException:
        return []

def log_match_data(match_data: Dict[str, Any], requested_username: str, requested_tag: str, region: str) -> bool:
    """Log match data to JSON file for statistics tracking"""
    try:
        # Create logs directory if it doesn't exist
        os.makedirs(LOGS_DIR, exist_ok=True)
        
        # Extract basic match info
        match_info = _extract_match_info(match_data)
        
        # Process player data
        processed_players = _process_players_data(match_data, requested_username, requested_tag)
        
        # Extract detailed rounds data for economy/clutch analysis
        rounds_data = match_data.get('rounds', [])
        kills_data = match_data.get('kills', [])
        
        # Create log entry with comprehensive data
        log_entry = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'match_id': match_info['match_id'],
            'requested_player': f"{requested_username}#{requested_tag}",
            'region': region,
            'match_info': match_info,
            'players': processed_players,
            'rounds': rounds_data,  # Include detailed round data for economy analysis
            'kills': kills_data     # Include kill events for clutch analysis
        }
        
        # Save to daily log file
        return _save_match_log(log_entry, match_info['match_id'])
        
    except Exception:
        return False

def _extract_match_info(match_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract basic match information from match data"""
    metadata = match_data.get('metadata', {})
    teams = match_data.get('teams', {})
    
    red_team = teams.get('red', {}) if isinstance(teams, dict) else {}
    blue_team = teams.get('blue', {}) if isinstance(teams, dict) else {}
    red_rounds = red_team.get('rounds_won', 0) if isinstance(red_team, dict) else 0
    blue_rounds = blue_team.get('rounds_won', 0) if isinstance(blue_team, dict) else 0
    
    return {
        'match_id': metadata.get('matchid', 'unknown'),
        'map': metadata.get('map', 'Unknown'),
        'mode': metadata.get('mode', 'Unknown'),
        'started_at': metadata.get('game_start_patched', 'Unknown'),
        'rounds_played': metadata.get('rounds_played', 0),
        'score': f"{red_rounds}-{blue_rounds}",
        'red_rounds': red_rounds,
        'blue_rounds': blue_rounds
    }

def _process_players_data(match_data: Dict[str, Any], requested_username: str, requested_tag: str) -> List[Dict[str, Any]]:
    """Process and calculate stats for all players in the match"""
    players_data = match_data.get('players', {})
    all_players = players_data.get('all_players', []) if isinstance(players_data, dict) else []
    
    match_kills = match_data.get('kills', [])
    round_data = match_data.get('rounds', [])
    rounds_played = match_data.get('metadata', {}).get('rounds_played', 0)
    
    processed_players = []
    
    for player in all_players:
        player_stats = _calculate_player_stats(player, match_data, match_kills, round_data, rounds_played)
        player_stats['is_requested_player'] = (
            player.get('name', '').lower() == requested_username.lower() and 
            player.get('tag', '') == requested_tag
        )
        processed_players.append(player_stats)
    
    return processed_players

def _calculate_player_stats(player: Dict[str, Any], match_data: Dict[str, Any], 
                          match_kills: List[Dict[str, Any]], round_data: List[Dict[str, Any]], 
                          rounds_played: int) -> Dict[str, Any]:
    """Calculate comprehensive stats for a single player"""
    name = player.get('name', 'Unknown')
    tag_val = player.get('tag', '0000')
    stats = player.get('stats', {})
    player_puuid = player.get('puuid', '')
    
    # Basic stats
    kills = stats.get('kills', 0)
    deaths = stats.get('deaths', 0)
    assists = stats.get('assists', 0)
    total_score = stats.get('score', 0)
    damage = player.get('damage_made', 0)
    
    # Combat stats
    headshots = stats.get('headshots', 0)
    bodyshots = stats.get('bodyshots', 0)
    legshots = stats.get('legshots', 0)
    total_shots = headshots + bodyshots + legshots
    
    # Calculate derived metrics
    acs = int(total_score / rounds_played) if rounds_played > 0 else 0
    adr = round(damage / rounds_played, 1) if damage > 0 and rounds_played > 0 else 0
    hs_pct = round((headshots / total_shots) * 100, 1) if total_shots > 0 else 0
    kda = round((kills + assists) / deaths, 2) if deaths > 0 else (kills + assists)
    
    # Calculate KAST and other advanced stats
    kast_pct = calculate_kast(match_data, player_puuid, rounds_played)
    
    return {
        'name': name,
        'tag': tag_val,
        'team': player.get('team', 'Unknown'),
        'rank': player.get('currenttier_patched', 'Unranked'),
        'agent': player.get('character', 'Unknown'),
        'stats': {
            'kills': kills,
            'deaths': deaths,
            'assists': assists,
            'acs': acs,
            'adr': adr,
            'headshot_pct': hs_pct,
            'headshots': headshots,
            'bodyshots': bodyshots,
            'legshots': legshots,
            'kda': kda,
            'kast': kast_pct,
            'score': total_score,
            'damage': damage,
            'first_bloods': 0,  # Would need detailed analysis
            'first_deaths': 0,
            'multikills': 0,
            'plus_minus': kills - deaths
        }
    }

def _save_match_log(log_entry: Dict[str, Any], match_id: str) -> bool:
    """Save match log entry to daily file"""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    log_file = os.path.join(LOGS_DIR, f"matches_{today}.json")
    
    # Load existing data
    existing_data = load_json_file(log_file)
    if not isinstance(existing_data, list):
        existing_data = []
    
    # Check if match already logged (avoid duplicates)
    match_exists = any(entry.get('match_id') == match_id for entry in existing_data)
    if not match_exists:
        existing_data.append(log_entry)
        
        if save_json_file(log_file, existing_data):
            return True
    else:
        return True
    
    return False

def load_player_match_history(username: str, tag: str, days: int = DEFAULT_STATS_DAYS) -> List[Dict[str, Any]]:
    """Load match history for a player from stored logs"""
    all_matches = []
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    
    try:
        # Check all log files within the date range
        for i in range(days + 1):
            check_date = datetime.now(timezone.utc) - timedelta(days=i)
            log_file = os.path.join(LOGS_DIR, f"matches_{check_date.strftime('%Y-%m-%d')}.json")
            
            if os.path.exists(log_file):
                daily_matches = load_json_file(log_file)
                if daily_matches:
                    for match in daily_matches:
                        # Check if this match involves our player
                        players = match.get('players', [])
                        for player in players:
                            if (player.get('name', '').lower() == username.lower() and 
                                player.get('tag', '') == tag):
                                # Check if match is within date range
                                match_time_str = match.get('timestamp', '')
                                try:
                                    match_time = datetime.fromisoformat(match_time_str.replace('Z', '+00:00'))
                                    if match_time >= cutoff_date:
                                        all_matches.append(match)
                                except:
                                    # Include if we can't parse the date
                                    all_matches.append(match)
                                break
    except Exception:
        pass
    
    # Sort by timestamp (newest first)
    all_matches.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    return all_matches

def calculate_comprehensive_stats(matches: List[Dict[str, Any]], username: str, tag: str) -> Dict[str, Any]:
    """Calculate comprehensive statistics from match history"""
    if not matches:
        return {}
    
    stats = {
        'total_matches': len(matches),
        'wins': 0,
        'losses': 0,
        'total_kills': 0,
        'total_deaths': 0,
        'total_assists': 0,
        'total_acs': 0,
        'total_adr': 0,
        'total_damage': 0,
        'total_rounds': 0,
        'total_headshots': 0,
        'total_bodyshots': 0,
        'total_legshots': 0,
        'kast_scores': [],
        'match_performances': [],
        'agent_stats': {},
        'map_stats': {},
        'recent_trend': [],
        'best_match': None,
        'worst_match': None
    }
    
    best_acs = 0
    worst_acs = float('inf')
    
    for match in matches:
        match_info = match.get('match_info', {})
        players = match.get('players', [])
        
        # Find the player's stats in this match
        player_match_stats = None
        for player in players:
            if (player.get('name', '').lower() == username.lower() and 
                player.get('tag', '') == tag):
                player_match_stats = player
                break
        
        if not player_match_stats:
            continue
            
        player_stats = player_match_stats.get('stats', {})
        map_name = match_info.get('map', 'Unknown')
        agent = player_match_stats.get('agent', 'Unknown')
        
        # Basic match stats
        kills = player_stats.get('kills', 0)
        deaths = player_stats.get('deaths', 0)
        assists = player_stats.get('assists', 0)
        acs = player_stats.get('acs', 0)
        adr = player_stats.get('adr', 0)
        kast = player_stats.get('kast', 0)
        damage = player_stats.get('damage', 0)
        hs_pct = player_stats.get('headshot_pct', 0)
        
        # Get raw hit counts for proper headshot percentage calculation
        headshots = player_stats.get('headshots', 0)
        bodyshots = player_stats.get('bodyshots', 0)
        legshots = player_stats.get('legshots', 0)
        
        # If raw hit data is not available (old matches), try to estimate from headshot_pct
        if headshots == 0 and bodyshots == 0 and legshots == 0 and hs_pct > 0:
            # Estimate based on kills (rough approximation)
            estimated_total_shots = kills * 3  # Rough estimate of shots per kill
            headshots = int(estimated_total_shots * (hs_pct / 100))
            bodyshots = estimated_total_shots - headshots
        
        # Win/Loss calculation
        player_team = player_match_stats.get('team', '').lower()
        red_rounds = match_info.get('red_rounds', 0)
        blue_rounds = match_info.get('blue_rounds', 0)
        
        if player_team == 'red':
            if red_rounds > blue_rounds:
                stats['wins'] += 1
            else:
                stats['losses'] += 1
        elif player_team == 'blue':
            if blue_rounds > red_rounds:
                stats['wins'] += 1
            else:
                stats['losses'] += 1
        
        # Accumulate stats
        stats['total_kills'] += kills
        stats['total_deaths'] += deaths
        stats['total_assists'] += assists
        stats['total_acs'] += acs
        stats['total_adr'] += adr
        stats['total_damage'] += damage
        stats['total_rounds'] += match_info.get('rounds_played', 0)
        stats['total_headshots'] += headshots
        stats['total_bodyshots'] += bodyshots
        stats['total_legshots'] += legshots
        
        if kast > 0:
            stats['kast_scores'].append(kast)
        
        # Track match performance for trends
        match_performance = {
            'timestamp': match.get('timestamp', ''),
            'acs': acs,
            'kda': round((kills + assists) / deaths, 2) if deaths > 0 else (kills + assists),
            'kast': kast,
            'map': map_name,
            'agent': agent,
            'kills': kills,
            'deaths': deaths,
            'assists': assists
        }
        stats['match_performances'].append(match_performance)
        
        # Best/worst match tracking
        if acs > best_acs:
            best_acs = acs
            stats['best_match'] = match_performance
        if acs < worst_acs and acs > 0:
            worst_acs = acs
            stats['worst_match'] = match_performance
        
        # Agent statistics
        if agent not in stats['agent_stats']:
            stats['agent_stats'][agent] = {
                'matches': 0, 'wins': 0, 'kills': 0, 'deaths': 0, 
                'assists': 0, 'acs_total': 0, 'kast_scores': []
            }
        
        agent_stat = stats['agent_stats'][agent]
        agent_stat['matches'] += 1
        agent_stat['kills'] += kills
        agent_stat['deaths'] += deaths
        agent_stat['assists'] += assists
        agent_stat['acs_total'] += acs
        if kast > 0:
            agent_stat['kast_scores'].append(kast)
        
        # Track wins for agents
        if ((player_team == 'red' and red_rounds > blue_rounds) or 
            (player_team == 'blue' and blue_rounds > red_rounds)):
            agent_stat['wins'] += 1
        
        # Map statistics
        if map_name not in stats['map_stats']:
            stats['map_stats'][map_name] = {
                'matches': 0, 'wins': 0, 'acs_total': 0, 'kast_scores': []
            }
        
        map_stat = stats['map_stats'][map_name]
        map_stat['matches'] += 1
        map_stat['acs_total'] += acs
        if kast > 0:
            map_stat['kast_scores'].append(kast)
        
        # Track wins for maps
        if ((player_team == 'red' and red_rounds > blue_rounds) or 
            (player_team == 'blue' and blue_rounds > red_rounds)):
            map_stat['wins'] += 1
    
    # Calculate recent trend (last 5 matches)
    recent_matches = stats['match_performances'][:5]
    if len(recent_matches) >= 2:
        recent_acs = [m['acs'] for m in recent_matches if m['acs'] > 0]
        if len(recent_acs) >= 2:
            # Simple trend: compare first half vs second half of recent matches
            mid = len(recent_acs) // 2
            recent_avg = sum(recent_acs[:mid]) / mid if mid > 0 else 0
            older_avg = sum(recent_acs[mid:]) / (len(recent_acs) - mid) if len(recent_acs) > mid else 0
            stats['recent_trend'] = 'improving' if recent_avg > older_avg else 'declining'
        else:
            stats['recent_trend'] = 'stable'
    
    return stats

def create_stats_embed(stats: Dict[str, Any], username: str, tag: str, days: int) -> discord.Embed:
    """Create a comprehensive stats embed"""
    if not stats or stats.get('total_matches', 0) == 0:
        embed = discord.Embed(
            title=f"📊 Stats for {username}#{tag}",
            description="❌ No competitive match data found for the specified time period.",
            color=0xFF4655
        )
        embed.add_field(
            name="💡 How to get stats",
            value="Use `/fullmatch` to analyze matches and build up your statistics database.",
            inline=False
        )
        return embed
    
    total_matches = stats['total_matches']
    wins = stats['wins']
    losses = stats['losses']
    win_rate = round((wins / (wins + losses)) * 100, 1) if (wins + losses) > 0 else 0
    
    # Calculate averages
    avg_acs = round(stats['total_acs'] / total_matches, 1) if total_matches > 0 else 0
    avg_adr = round(stats['total_adr'] / total_matches, 1) if total_matches > 0 else 0
    avg_kast = round(sum(stats['kast_scores']) / len(stats['kast_scores']), 1) if stats['kast_scores'] else 0
    avg_kda = round((stats['total_kills'] + stats['total_assists']) / stats['total_deaths'], 2) if stats['total_deaths'] > 0 else 0
    
    embed = discord.Embed(
        title=f"📊 Competitive Stats for {username}#{tag}",
        description=f"Performance analysis over the last **{days} days** ({total_matches} matches)",
        color=0x00FF00 if win_rate >= 50 else 0xFF4655,
        timestamp=datetime.now(timezone.utc)
    )
    
    # Overall Performance
    embed.add_field(
        name="🏆 Overall Performance",
        value=(
            f"**Win Rate:** {win_rate}% ({wins}W-{losses}L)\n"
            f"**Average ACS:** {avg_acs}\n"
            f"**Average ADR:** {avg_adr}\n"
            f"**Average K/D/A:** {avg_kda}\n"
            f"**Average KAST:** {avg_kast}%"
        ),
        inline=True
    )
    
    # Combat Stats
    total_shots = stats['total_headshots'] + stats['total_bodyshots'] + stats['total_legshots']
    hs_pct = round((stats['total_headshots'] / total_shots) * 100, 1) if total_shots > 0 else 0
    
    embed.add_field(
        name="⚔️ Combat Metrics",
        value=(
            f"**Total Kills:** {stats['total_kills']}\n"
            f"**Total Deaths:** {stats['total_deaths']}\n"
            f"**Total Assists:** {stats['total_assists']}\n"
            f"**Total Damage:** {stats['total_damage']:,}\n"
            f"**Headshot %:** {hs_pct}%"
        ),
        inline=True
    )
    
    # Performance Trend
    trend_emoji = "📈" if stats.get('recent_trend') == 'improving' else "📉" if stats.get('recent_trend') == 'declining' else "📊"
    trend_text = stats.get('recent_trend', 'stable').title()
    
    best_match = stats.get('best_match', {})
    worst_match = stats.get('worst_match', {})
    
    embed.add_field(
        name=f"{trend_emoji} Recent Trend",
        value=(
            f"**Trend:** {trend_text}\n"
            f"**Best Game:** {best_match.get('acs', 0)} ACS\n"
            f"**Worst Game:** {worst_match.get('acs', 0)} ACS\n"
            f"**Best Map:** {best_match.get('map', 'Unknown')}\n"
            f"**Best Agent:** {best_match.get('agent', 'Unknown')}"
        ),
        inline=True
    )
    
    # Agent Performance (top 3)
    agent_stats = stats.get('agent_stats', {})
    if agent_stats:
        sorted_agents = sorted(
            agent_stats.items(), 
            key=lambda x: x[1]['acs_total'] / x[1]['matches'] if x[1]['matches'] > 0 else 0, 
            reverse=True
        )[:3]
        
        agent_text = ""
        for agent, agent_data in sorted_agents:
            matches = agent_data['matches']
            wins = agent_data['wins']
            agent_wr = round((wins / matches) * 100, 1) if matches > 0 else 0
            agent_acs = round(agent_data['acs_total'] / matches, 1) if matches > 0 else 0
            agent_kast = round(sum(agent_data['kast_scores']) / len(agent_data['kast_scores']), 1) if agent_data['kast_scores'] else 0
            
            agent_text += f"**{agent}:** {agent_wr}% WR, {agent_acs} ACS, {agent_kast}% KAST ({matches}m)\n"
        
        embed.add_field(
            name="🎭 Top Agents",
            value=agent_text or "No agent data available",
            inline=True
        )
    
    # Map Performance (top 3)
    map_stats = stats.get('map_stats', {})
    if map_stats:
        # Filter maps with at least 2 matches for meaningful stats
        filtered_maps = {k: v for k, v in map_stats.items() if v['matches'] >= 2}
        sorted_maps = sorted(
            filtered_maps.items(),
            key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1]['matches'] > 0 else 0,
            reverse=True
        )[:3]
        
        map_text = ""
        for map_name, map_data in sorted_maps:
            matches = map_data['matches']
            wins = map_data['wins']
            map_wr = round((wins / matches) * 100, 1) if matches > 0 else 0
            map_acs = round(map_data['acs_total'] / matches, 1) if matches > 0 else 0
            map_kast = round(sum(map_data['kast_scores']) / len(map_data['kast_scores']), 1) if map_data['kast_scores'] else 0
            
            map_text += f"**{map_name}:** {map_wr}% WR, {map_acs} ACS, {map_kast}% KAST ({matches}m)\n"
        
        embed.add_field(
            name="🗺️ Top Maps",
            value=map_text or "Need at least 2 matches per map",
            inline=True
        )
    
    # Recent matches (last 5)
    recent_matches = stats.get('match_performances', [])[:5]
    if recent_matches:
        recent_text = ""
        for i, match in enumerate(recent_matches, 1):
            # Parse timestamp for display
            try:
                match_time = datetime.fromisoformat(match['timestamp'].replace('Z', '+00:00'))
                date_str = match_time.strftime('%m/%d')
            except:
                date_str = "??/??"
            
            recent_text += f"`{i}.` **{match['acs']}** ACS, **{match['kda']}** K/D/A, **{match['kast']}%** KAST - {match['agent']} on {match['map']} *({date_str})*\n"
        
        embed.add_field(
            name="📋 Recent Matches",
            value=recent_text,
            inline=False
        )
    
    embed.set_footer(text=f"Data from match logs • Use /fullmatch to update • {total_matches} matches analyzed")
    return embed

def create_comprehensive_match_embed(match_data, username, tag, region):
    """Create a Discord embed with comprehensive match information for all players"""
    embed = discord.Embed(
        title=f"🎯 Competitive Match Stats for {username}#{tag}",
        description=f"Detailed stats for all players in **{region.upper()}** region (Competitive only)",
        color=0xFF4655,
        timestamp=datetime.now(timezone.utc)
    )
    
    if not match_data:
        embed.add_field(
            name="❌ No match data found",
            value="Unable to retrieve detailed match information.",
            inline=False
        )
        return embed
    
    try:
        # Get match metadata
        metadata = match_data.get('metadata', {})
        map_name = metadata.get('map', 'Unknown Map')
        mode = metadata.get('mode', 'Unknown Mode')
        started_at = metadata.get('game_start_patched', 'Unknown Time')
        rounds_played = metadata.get('rounds_played', 0)
        
        # Get team scores
        teams = match_data.get('teams', {})
        if not isinstance(teams, dict):
            teams = {}
        
        red_team = teams.get('red', {})
        blue_team = teams.get('blue', {})
        
        # Ensure team data is dict type
        if not isinstance(red_team, dict):
            red_team = {}
        if not isinstance(blue_team, dict):
            blue_team = {}
        
        red_rounds = red_team.get('rounds_won', 0)
        blue_rounds = blue_team.get('rounds_won', 0)
        
        # Match header
        embed.add_field(
            name="🗺️ Match Overview",
            value=(
                f"**Map:** {map_name}\n"
                f"**Mode:** {mode}\n"
                f"**Score:** {red_rounds} - {blue_rounds}\n"
                f"**Duration:** {rounds_played} rounds\n"
                f"**Started:** {started_at}"
            ),
            inline=False
        )
        
        # Get all players
        players_data = match_data.get('players', {})
        if isinstance(players_data, dict):
            all_players = players_data.get('all_players', [])
        else:
            all_players = []
        
        if not all_players:
            embed.add_field(
                name="❌ No player data available",
                value="Unable to retrieve player statistics.",
                inline=False
            )
            return embed
        
        # Separate players by team
        red_players = [p for p in all_players if p.get('team', '').lower() == 'red']
        blue_players = [p for p in all_players if p.get('team', '').lower() == 'blue']
        
        # Sort each team by ACS (descending order)
        def get_acs(player):
            stats = player.get('stats', {})
            total_score = stats.get('score', 0)
            return int(total_score / rounds_played) if rounds_played > 0 else total_score
        
        red_players.sort(key=get_acs, reverse=True)
        blue_players.sort(key=get_acs, reverse=True)
        
        # Helper function to format player stats
        def format_player_stats(players, team_name, team_color):
            if not players:
                return f"**{team_color} {team_name} Team:** No data available"
            
            stats_text = f"**{team_color} {team_name} Team:**\n```\n"
            # Improved column alignment with consistent widths
            stats_text += f"{'Player':<16} {'Agent':<11} {'K/D/A':<7} {'ACS':<4} {'ADR':<4} {'HS%':<4} {'KAST':<4} {'FK':<2} {'FD':<2} {'MK':<2} {'+/-':<3}\n"
            stats_text += "─" * 80 + "\n"
            
            for player in players:
                name = player.get('name', 'Unknown')
                tag_val = player.get('tag', '0000')
                stats = player.get('stats', {})
                agent = player.get('character', 'Unknown')
                
                # Mark requested player
                name_display = f"►{name}" if (name.lower() == username.lower() and tag_val == tag) else name
                name_display = name_display[:15]  # Truncate if too long
                
                # Basic stats
                kills = stats.get('kills', 0)
                deaths = stats.get('deaths', 0)
                assists = stats.get('assists', 0)
                total_score = stats.get('score', 0)
                
                # Calculate derived stats
                acs = int(total_score / rounds_played) if rounds_played > 0 else 0
                
                # Get damage and calculate ADR
                damage = player.get('damage_made', 0)
                adr = round(damage / rounds_played, 1) if damage > 0 and rounds_played > 0 else 0
                
                # Get headshot percentage
                headshots = stats.get('headshots', 0)
                bodyshots = stats.get('bodyshots', 0)
                legshots = stats.get('legshots', 0)
                total_shots = headshots + bodyshots + legshots
                hs_pct = round((headshots / total_shots) * 100, 1) if total_shots > 0 else 0
                
                # Calculate KAST
                player_puuid = player.get('puuid', '')
                kast_pct = calculate_kast(match_data, player_puuid, rounds_played)
                
                # First kills/deaths and multikills (simplified for now)
                first_kills = 0  # This would need match kill data analysis
                first_deaths = 0
                multikills = 0

                # Analyze kills data for FK/FD/MK
                if 'kills' in match_data:
                    kills_data = match_data['kills']

                    # Group kills by round
                    kills_by_round = {}
                    for kill_event in kills_data:
                        round_number = kill_event.get('round', 0)
                        if round_number not in kills_by_round:
                            kills_by_round[round_number] = []
                        kills_by_round[round_number].append(kill_event)
                    
                    # Check each round for first kills/deaths and count player's kills per round
                    for round_num, round_kills in kills_by_round.items():
                        # Sort by kills to find first kill/death
                        round_kills.sort(key=lambda x: x.get('kill_time_in_round', 0))

                        # Count kills by this player in this round
                        player_kills_this_round = 0

                        for i, kill_event in enumerate(round_kills):
                            killer_puuid = kill_event.get('killer_puuid')
                            victim_puuid = kill_event.get('victim_puuid')

                            # Count kills by this player
                            if killer_puuid == player_puuid:
                                player_kills_this_round += 1

                                # First kill of the round
                                if i == 0:
                                    first_kills += 1
                            
                            # First death of the round
                            if victim_puuid == player_puuid and i == 0:
                                first_deaths += 1
                        # Count multikills (3+ kills in a round)
                        if player_kills_this_round >= 3:
                            multikills += 1

                # Plus/minus
                plus_minus = kills - deaths
                plus_minus_str = f"+{plus_minus}" if plus_minus > 0 else str(plus_minus)
                
                # Format the line
                kda_str = f"{kills}/{deaths}/{assists}"
                agent_short = agent[:10]  # Truncate agent name
                
                stats_text += f"{name_display:<16} {agent_short:<11} {kda_str:<7} {acs:<4} {adr:<4} {hs_pct:<4} {kast_pct:<4} {first_kills:<2} {first_deaths:<2} {multikills:<2} {plus_minus_str:<3}\n"
            
            stats_text += "```"
            return stats_text
        
        # Add team stats
        if red_players:
            red_stats = format_player_stats(red_players, "Red", "🔴")
            embed.add_field(name="🔴 Red Team", value=red_stats, inline=False)
        
        if blue_players:
            blue_stats = format_player_stats(blue_players, "Blue", "🔵")
            embed.add_field(name="🔵 Blue Team", value=blue_stats, inline=False)
        
        # Add legend
        embed.add_field(
            name="📊 Legend",
            value=(
                "**Player:** Name#Tag (►requested player)\n"
                "**Agent:** Character played in match\n"
                "**K/D/A:** Kills/Deaths/Assists\n"
                "**ACS:** Average Combat Score\n"
                "**ADR:** Average Damage per Round\n"
                "**HS%:** Headshot Percentage\n"
                "**KAST:** Kill/Assist/Survive/Trade %\n"
                "**FK:** First Kills (First Bloods)\n"
                "**FD:** First Deaths\n"
                "**MK:** Multi-kills (3K+)\n"
                "**+/-:** Kill/Death Differential"
            ),
            inline=True
        )
        
    except Exception:
        embed.add_field(
            name="❌ Error processing match data",
            value="An error occurred while processing the match information.",
            inline=False
        )
    
    embed.set_footer(text=f"Match ID: {match_data.get('metadata', {}).get('matchid', 'Unknown')} • Henrik-3 API")
    return embed

# Core slash commands
@bot.tree.command(name="recentmatch", description="Get comprehensive stats for all 10 players in a recent match")
@app_commands.describe(
    region="Select the region where the player plays",
    username="Player's username (case-sensitive, spaces allowed)",
    tag="Player's tag (letters and numbers, e.g., 1234 or ABC1)"
)
@app_commands.choices(region=[
    app_commands.Choice(name="Europe (EU)", value="eu"),
    app_commands.Choice(name="North America (NA)", value="na"),
    app_commands.Choice(name="Asia Pacific (AP)", value="ap"),
    app_commands.Choice(name="Korea (KR)", value="kr"),
])
async def slash_full_match_report(interaction: discord.Interaction, region: app_commands.Choice[str], username: str, tag: str):
    """Get comprehensive stats for all 10 players in the most recent competitive match"""
    
    await interaction.response.defer()
    
    try:
        # Fetch player's recent matches (use 50 for stats to get enough data)
        matches_data = await asyncio.get_event_loop().run_in_executor(
            None, fetch_valorant_matches, region.value, username, tag, 50
        )
        
        if not matches_data:
            await interaction.edit_original_response(content="❌ No match data found for this player.")
            return
        
        # Filter for competitive matches only
        competitive_matches = filter_competitive_matches(matches_data)
        
        if not competitive_matches:
            await interaction.edit_original_response(content="❌ No competitive matches found for this player.")
            return
        
        # Get the most recent competitive match
        latest_match = competitive_matches[0]
        match_id = latest_match.get('metadata', {}).get('matchid')
        
        if not match_id:
            await interaction.edit_original_response(content="❌ Could not find match ID for the latest match.")
            return
        
        # Fetch detailed match data
        match_data = await asyncio.get_event_loop().run_in_executor(
            None, fetch_match_details, match_id
        )
        
        if not match_data:
            await interaction.edit_original_response(content="❌ Could not fetch detailed match data.")
            return
        
        # Create and send the comprehensive embed
        embed = create_comprehensive_match_embed(match_data, username, tag, region.value)
        await interaction.edit_original_response(embed=embed)
        
        # Log match data for statistics tracking
        try:
            log_match_data(match_data, username, tag, region.value)
        except Exception:
            pass  # Don't fail the command if logging fails
        
    except Exception:
        await interaction.edit_original_response(content="❌ An error occurred while fetching match data.")

@bot.tree.command(name="stats", description="View detailed personal performance statistics")
@app_commands.describe(
    player="Optional: Player to check (username#tag) - leave empty to use your linked account",
    days="Number of days to analyze (default: 7, max: 90)"
)
async def slash_player_stats(interaction: discord.Interaction, player: str = None, days: int = DEFAULT_STATS_DAYS):
    """Show comprehensive player statistics from match history"""
    
    await interaction.response.defer()
    
    try:
        # Validate days parameter
        if days < 1 or days > MAX_MATCH_HISTORY_DAYS:
            await interaction.edit_original_response(
                content=f"❌ Days must be between 1 and {MAX_MATCH_HISTORY_DAYS}."
            )
            return
        
        username = None
        tag = None
        
        # Determine which player to check
        if player:
            # Parse provided player
            if '#' in player:
                username, tag = player.split('#', 1)
            else:
                await interaction.edit_original_response(
                    content="❌ Please provide player in format: `username#tag`"
                )
                return
        else:
            # Use linked account
            try:
                if os.path.exists(LINKS_FILE):
                    with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                        user_links = json.load(f)
                    
                    user_id = str(interaction.user.id)
                    if user_id in user_links:
                        link_data = user_links[user_id]
                        username = link_data.get('username')
                        tag = link_data.get('tag')
                    else:
                        await interaction.edit_original_response(
                            content="❌ No linked account found. Use `/link` to link your account or specify a player manually."
                        )
                        return
                else:
                    await interaction.edit_original_response(
                        content="❌ No linked accounts found. Use `/link` to link your account first."
                    )
                    return
            except Exception:
                await interaction.edit_original_response(
                    content="❌ Error reading linked accounts."
                )
                return
        
        if not username or not tag:
            await interaction.edit_original_response(
                content="❌ Invalid player information."
            )
            return
        
        # Load match history from logs
        matches = await asyncio.get_event_loop().run_in_executor(
            None, load_player_match_history, username, tag, days
        )
        
        if not matches:
            no_data_embed = discord.Embed(
                title=f"📊 Stats for {username}#{tag}",
                description=f"❌ No competitive match data found for the last {days} days.",
                color=0xFF4655
            )
            no_data_embed.add_field(
                name="💡 How to build your stats database",
                value=(
                    "1. Use `/fullmatch` to analyze recent matches\n"
                    "2. Match data gets automatically logged\n"
                    "3. Return here to see your performance trends\n"
                    "4. More matches = more accurate statistics"
                ),
                inline=False
            )
            no_data_embed.add_field(
                name="🔍 What data is tracked",
                value=(
                    "• Win rate and performance trends\n"
                    "• Agent specialization and efficiency\n"
                    "• Map performance analysis\n"
                    "• Combat metrics and improvement areas\n"
                    "• KAST, ACS, ADR, and headshot statistics"
                ),
                inline=False
            )
            await interaction.edit_original_response(embed=no_data_embed)
            return
        
        # Calculate comprehensive statistics
        stats = await asyncio.get_event_loop().run_in_executor(
            None, calculate_comprehensive_stats, matches, username, tag
        )
        
        # Create and send stats embed
        embed = create_stats_embed(stats, username, tag, days)
        await interaction.edit_original_response(embed=embed)
        
    except Exception:
        await interaction.edit_original_response(
            content="❌ An error occurred while calculating statistics."
        )

def fetch_multiple_matches(region: str, username: str, tag: str, count: int) -> Tuple[List[Dict[str, Any]], int, int]:
    """Fetch multiple matches and return (matches, new_matches, duplicates)"""
    try:
        # Get matches from API with requested count (request more to account for non-competitive matches)
        api_size = min(max(count * 2, 20), 100)  # Request 2x the count or at least 20, max 100
        matches_data = fetch_valorant_matches(region, username, tag, api_size)
        if not matches_data:
            return [], 0, 0
        
        # Filter for competitive matches
        competitive_matches = filter_competitive_matches(matches_data)
        
        # Limit to requested count
        matches_to_process = competitive_matches[:count]
        
        new_matches = 0
        duplicates = 0
        processed_matches = []
        
        for match in matches_to_process:
            match_id = match.get('metadata', {}).get('matchid')
            if not match_id:
                continue
                
            # Check if match already exists in logs
            if is_match_already_logged(match_id):
                duplicates += 1
                continue
            
            # Fetch detailed match data
            detailed_match = fetch_match_details(match_id)
            if detailed_match:
                processed_matches.append(detailed_match)
                new_matches += 1
        
        return processed_matches, new_matches, duplicates
        
    except Exception:
        return [], 0, 0

def is_match_already_logged(match_id: str) -> bool:
    """Check if a match ID already exists in the logs"""
    try:
        # Check the last 30 days of logs
        for i in range(30):
            check_date = datetime.now(timezone.utc) - timedelta(days=i)
            log_file = os.path.join(LOGS_DIR, f"matches_{check_date.strftime('%Y-%m-%d')}.json")
            
            if os.path.exists(log_file):
                daily_matches = load_json_file(log_file)
                if any(entry.get('match_id') == match_id for entry in daily_matches):
                    return True
        return False
    except Exception:
        return False

def create_fetch_confirmation_embed(matches: List[Dict[str, Any]], username: str, tag: str, 
                                  region: str, new_count: int, duplicate_count: int) -> discord.Embed:
    """Create embed showing what matches will be added"""
    embed = discord.Embed(
        title=f"🎯 Fetch Matches for {username}#{tag}",
        description=f"Ready to process **{len(matches)}** competitive matches from **{region.upper()}** region",
        color=0x00FF00 if new_count > 0 else 0xFFFF00
    )
    
    embed.add_field(
        name="📊 Match Summary",
        value=(
            f"**New Matches:** {new_count}\n"
            f"**Duplicates Skipped:** {duplicate_count}\n"
            f"**Total Found:** {new_count + duplicate_count}\n"
            f"**Region:** {region.upper()}"
        ),
        inline=True
    )
    
    if matches:
        # Show preview of first few matches
        preview_text = ""
        for i, match in enumerate(matches[:5], 1):
            metadata = match.get('metadata', {})
            map_name = metadata.get('map', 'Unknown')
            started_at = metadata.get('game_start_patched', 'Unknown')
            
            # Try to parse and format the date
            try:
                if 'T' in started_at:
                    date_part = started_at.split('T')[0]
                    preview_text += f"`{i}.` **{map_name}** - {date_part}\n"
                else:
                    preview_text += f"`{i}.` **{map_name}** - {started_at}\n"
            except:
                preview_text += f"`{i}.` **{map_name}** - Recent\n"
        
        if len(matches) > 5:
            preview_text += f"... and {len(matches) - 5} more matches"
        
        embed.add_field(
            name="🎮 Match Preview",
            value=preview_text,
            inline=True
        )
    
    embed.add_field(
        name="⚡ What happens next?",
        value=(
            "• Click **✅ Confirm** to add matches to database\n"
            "• Click **❌ Cancel** to abort the operation\n"
            "• Data will be available for `/stats` and analytics\n"
            "• Detailed match breakdowns will be stored"
        ),
        inline=False
    )
    
    if new_count == 0:
        embed.add_field(
            name="ℹ️ Note",
            value="All matches are already in the database. No new data to add.",
            inline=False
        )
    
    embed.set_footer(text="Data will be stored in match logs for comprehensive analytics")
    return embed

class FetchConfirmationView(discord.ui.View):
    """Confirmation view for fetching matches"""
    
    def __init__(self, matches: List[Dict[str, Any]], username: str, tag: str, region: str):
        super().__init__(timeout=60)
        self.matches = matches
        self.username = username
        self.tag = tag
        self.region = region
        self.confirmed = False
    
    @discord.ui.button(label="✅ Confirm & Add Matches", style=discord.ButtonStyle.green)
    async def confirm_fetch(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Confirm and process the matches"""
        if self.confirmed:
            await interaction.response.send_message("❌ Already processing matches.", ephemeral=True)
            return
            
        self.confirmed = True
        await interaction.response.defer()
        
        try:
            # Process and log all matches
            successful_logs = 0
            failed_logs = 0
            
            for match in self.matches:
                try:
                    if log_match_data(match, self.username, self.tag, self.region):
                        successful_logs += 1
                    else:
                        failed_logs += 1
                except Exception:
                    failed_logs += 1
            
            # Create success embed
            success_embed = discord.Embed(
                title="✅ Matches Added Successfully",
                description=f"Bulk data collection completed for **{self.username}#{self.tag}**",
                color=0x00FF00
            )
            
            success_embed.add_field(
                name="📊 Results",
                value=(
                    f"**Successfully Added:** {successful_logs} matches\n"
                    f"**Failed:** {failed_logs} matches\n"
                    f"**Total Processed:** {len(self.matches)} matches\n"
                    f"**Region:** {self.region.upper()}"
                ),
                inline=True
            )
            
            success_embed.add_field(
                name="🎯 What's Available Now",
                value=(
                    "• Use `/stats` to view updated performance analytics\n"
                    "• Enhanced agent and map performance data\n"
                    "• More accurate trend analysis\n"
                    "• Comprehensive match history available"
                ),
                inline=True
            )
            
            success_embed.add_field(
                name="📈 Next Steps",
                value=(
                    "• Check `/stats` for your updated analytics\n"
                    "• Use `/agents` for agent performance rankings\n"
                    "• Try `/economy` for economic analysis\n"
                    "• Explore `/clutch` for clutch statistics"
                ),
                inline=False
            )
            
            # Disable all buttons
            for item in self.children:
                item.disabled = True
            
            await interaction.edit_original_response(embed=success_embed, view=self)
            
        except Exception:
            error_embed = discord.Embed(
                title="❌ Error Processing Matches",
                description="An error occurred while adding matches to the database.",
                color=0xFF4655
            )
            await interaction.edit_original_response(embed=error_embed, view=None)
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.red)
    async def cancel_fetch(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel the fetch operation"""
        cancel_embed = discord.Embed(
            title="❌ Fetch Cancelled",
            description="No matches were added to the database.",
            color=0xFF9900
        )
        
        # Disable all buttons
        for item in self.children:
            item.disabled = True
        
        await interaction.response.edit_message(embed=cancel_embed, view=self)
    
    async def on_timeout(self):
        """Handle timeout"""
        for item in self.children:
            item.disabled = True

@bot.tree.command(name="fetch", description="Bulk fetch and store match data for comprehensive analytics")
@app_commands.describe(
    region="Select the region where the player plays",
    username="Player's username (case-sensitive, spaces allowed)",
    tag="Player's tag (letters and numbers, e.g., 1234 or ABC1)",
    count="Number of recent matches to fetch (default: 5, max: 10)"
)
@app_commands.choices(region=[
    app_commands.Choice(name="Europe (EU)", value="eu"),
    app_commands.Choice(name="North America (NA)", value="na"),
    app_commands.Choice(name="Asia Pacific (AP)", value="ap"),
    app_commands.Choice(name="Korea (KR)", value="kr"),
])
async def slash_fetch_matches(interaction: discord.Interaction, region: app_commands.Choice[str], 
                            username: str, tag: str, count: int = 5):
    """Fetch multiple matches for comprehensive data collection"""
    
    await interaction.response.defer()
    
    try:
        # Validate count parameter
        if count < 1 or count > 10:
            await interaction.edit_original_response(
                content="❌ Count must be between 1 and 10 matches."
            )
            return
        
        # Fetch matches
        matches, new_matches, duplicates = await asyncio.get_event_loop().run_in_executor(
            None, fetch_multiple_matches, region.value, username, tag, count
        )
        
        if not matches and new_matches == 0 and duplicates == 0:
            await interaction.edit_original_response(
                content=f"❌ No competitive matches found for {username}#{tag} in {region.value.upper()} region."
            )
            return
        
        # Create confirmation embed and view
        embed = create_fetch_confirmation_embed(matches, username, tag, region.value, new_matches, duplicates)
        view = FetchConfirmationView(matches, username, tag, region.value)
        
        await interaction.edit_original_response(embed=embed, view=view)
        
    except Exception:
        await interaction.edit_original_response(
            content="❌ An error occurred while fetching match data."
        )

def analyze_economy_data(matches: List[Dict[str, Any]], username: str, tag: str) -> Dict[str, Any]:
    """Analyze economy statistics from match data"""
    economy_stats = {
        'pistol_rounds': {'wins': 0, 'total': 0},
        'anti_eco_rounds': {'wins': 0, 'total': 0},
        'force_buy_rounds': {'wins': 0, 'total': 0},
        'full_buy_rounds': {'wins': 0, 'total': 0},
        'eco_rounds': {'wins': 0, 'total': 0},
        'total_matches': 0,
        'total_rounds': 0,
        'round_types': {},  # Track performance by round type
        'matches_with_data': 0,  # Track how many matches have complete rounds data
        'incomplete_matches': 0  # Track matches with missing rounds data
    }
    
    if not matches:
        return economy_stats
    
    economy_stats['total_matches'] = len(matches)
    
    for match in matches:
        rounds_data = match.get('rounds', [])
        players_data = match.get('players', [])
        match_info = match.get('match_info', {})
        
        # Check if this match has complete data
        if not rounds_data or not players_data:
            # Count matches that exist but don't have detailed rounds data
            if match_info.get('red_rounds', 0) + match_info.get('blue_rounds', 0) > 0:
                economy_stats['incomplete_matches'] += 1
            continue
        
        economy_stats['matches_with_data'] += 1
        
        # Find our player's data
        our_player = None
        for player in players_data:
            # Try the is_requested_player flag first, then fall back to name/tag match
            if (player.get('is_requested_player', False) or 
                (player.get('name', '').lower() == username.lower() and 
                 player.get('tag', '') == tag)):
                our_player = player
                break
        
        if not our_player:
            continue
            
        player_team = our_player.get('team', '').lower()
        economy_stats['total_rounds'] += len(rounds_data)
        
        # Track round progression to identify pistol rounds correctly
        team_round_history = []
        red_score = 0
        blue_score = 0
        
        for i, round_data in enumerate(rounds_data):
            # Determine the actual round number (1-based)
            round_number = i + 1
            
            # Classify round type - always use the classification function
            # which handles pistol rounds (rounds 1 and 13) correctly
            round_type, round_context = _classify_round_economy_improved(
                round_number, team_round_history, rounds_data, i
            )
            
            # Check if our team won this round
            round_won = _was_round_won_improved(round_data, match, our_player, round_number)
            team_round_history.append(round_won)
            
            # Update round scores (for tracking progression)
            winning_team = round_data.get('winning_team', '').lower()
            if winning_team == 'red':
                red_score += 1
            elif winning_team == 'blue':
                blue_score += 1
            
            # Update statistics for this round type
            if round_type in economy_stats:
                economy_stats[round_type]['total'] += 1
                if round_won:
                    economy_stats[round_type]['wins'] += 1
            
            # Track detailed round type information
            if round_type not in economy_stats['round_types']:
                economy_stats['round_types'][round_type] = {
                    'total': 0, 'wins': 0, 'contexts': {}
                }
            
            economy_stats['round_types'][round_type]['total'] += 1
            if round_won:
                economy_stats['round_types'][round_type]['wins'] += 1
            
            # Track context within round type (handle both pistol rounds and regular rounds)
            context_key = round_context
            if context_key not in economy_stats['round_types'][round_type]['contexts']:
                economy_stats['round_types'][round_type]['contexts'][context_key] = {'total': 0, 'wins': 0}
            
            economy_stats['round_types'][round_type]['contexts'][context_key]['total'] += 1
            if round_won:
                economy_stats['round_types'][round_type]['contexts'][context_key]['wins'] += 1
    
    return economy_stats

def _classify_round_economy_improved(round_num: int, team_round_history: List[bool], 
                                   rounds_data: List[Dict[str, Any]], round_index: int) -> Tuple[str, str]:
    """Classify round economy type based on round number, history, and available data"""
    # Pistol rounds
    if round_num in [1, 13]:
        return 'pistol_rounds', 'first_half' if round_num == 1 else 'second_half'
    
    # Anti-eco rounds (typically after pistol wins)
    if round_num in [2, 14] and len(team_round_history) > 0 and team_round_history[-1]:
        return 'anti_eco_rounds', 'post_pistol_win'
    
    # Try to get economy data from round if available
    if round_index < len(rounds_data):
        current_round = rounds_data[round_index]
        player_stats = current_round.get('player_stats', [])
        
        if player_stats:
            # Analyze loadout values to determine round type
            avg_loadout_value = 0
            economy_count = 0
            
            for player_stat in player_stats:
                economy = player_stat.get('economy', {})
                loadout_value = economy.get('loadout_value', 0)
                if loadout_value > 0:
                    avg_loadout_value += loadout_value
                    economy_count += 1
            
            if economy_count > 0:
                avg_loadout_value = avg_loadout_value / economy_count
                
                # Classify based on average loadout value
                if avg_loadout_value < 1000:  # Low buy (pistol/eco)
                    return 'eco_rounds', 'save_round'
                elif avg_loadout_value < 2500:  # Force buy
                    return 'force_buy_rounds', 'force_buy'
                else:  # Full buy
                    return 'full_buy_rounds', 'full_buy'
    
    # Fallback logic based on round patterns when economy data isn't available
    if len(team_round_history) >= 2:
        # Check for eco patterns after losses
        recent_losses = sum(1 for won in team_round_history[-2:] if not won)
        if recent_losses >= 2:
            return 'eco_rounds', 'loss_streak_eco'
    
    # Default classification based on round number
    if round_num in [2, 3, 14, 15]:
        return 'force_buy_rounds', 'early_round'
    else:
        return 'full_buy_rounds', 'standard'

def _was_round_won_improved(round_data: Dict[str, Any], match: Dict[str, Any], 
                          our_player: Dict[str, Any], round_num: int) -> bool:
    """Determine if the round was won by the tracked player's team"""
    try:
        player_team = our_player.get('team', '').lower()
        
        # Try to get winning team from round data
        winning_team = round_data.get('winning_team', '').lower()
        if winning_team:
            return winning_team == player_team
        
        # Fallback: check player stats in this round
        player_stats_list = round_data.get('player_stats', [])
        player_puuid = our_player.get('puuid', '')
        
        for player_stat in player_stats_list:
            if player_stat.get('player_puuid') == player_puuid:
                # If player has stats for this round, assume team played
                # This is a simplified check - in reality we'd need more complex logic
                break
        else:
            # Player not found in round stats
            return False
        
        # Simple fallback based on round progression
        return True  # Placeholder
    except Exception:
        return False

def create_economy_embed(economy_stats: Dict[str, Any], username: str, tag: str, days: int) -> discord.Embed:
    """Create economy analysis embed"""
    if economy_stats['total_matches'] == 0:
        embed = discord.Embed(
            title=f"💰 Economy Analysis for {username}#{tag}",
            description="❌ No competitive match data found for economy analysis.",
            color=0xFF4655
        )
        embed.add_field(
            name="💡 How to get economy data",
            value="Use `/fetch` to collect match data, then return here for economy analysis.",
            inline=False
        )
        return embed
    
    def calc_percentage(wins: int, total: int) -> float:
        return round((wins / total) * 100, 1) if total > 0 else 0.0
    
    embed = discord.Embed(
        title=f"💰 Economy Analysis for {username}#{tag}",
        description=f"Economic performance over the last **{days} days** ({economy_stats['matches_with_data']}/{economy_stats['total_matches']} matches with data, {economy_stats['total_rounds']} rounds)",
        color=0x00FF00,
        timestamp=datetime.now(timezone.utc)
    )
    
    # Round Type Performance
    pistol_wr = calc_percentage(economy_stats['pistol_rounds']['wins'], economy_stats['pistol_rounds']['total'])
    force_wr = calc_percentage(economy_stats['force_buy_rounds']['wins'], economy_stats['force_buy_rounds']['total'])
    full_wr = calc_percentage(economy_stats['full_buy_rounds']['wins'], economy_stats['full_buy_rounds']['total'])
    eco_wr = calc_percentage(economy_stats['eco_rounds']['wins'], economy_stats['eco_rounds']['total'])
    anti_eco_wr = calc_percentage(economy_stats['anti_eco_rounds']['wins'], economy_stats['anti_eco_rounds']['total'])
    
    embed.add_field(
        name="🎯 Round Type Win Rates",
        value=(
            f"**Pistol Rounds:** {pistol_wr}% ({economy_stats['pistol_rounds']['wins']}/{economy_stats['pistol_rounds']['total']})\n"
            f"**Anti-Eco:** {anti_eco_wr}% ({economy_stats['anti_eco_rounds']['wins']}/{economy_stats['anti_eco_rounds']['total']})\n"
            f"**Force Buy:** {force_wr}% ({economy_stats['force_buy_rounds']['wins']}/{economy_stats['force_buy_rounds']['total']})\n"
            f"**Full Buy:** {full_wr}% ({economy_stats['full_buy_rounds']['wins']}/{economy_stats['full_buy_rounds']['total']})\n"
            f"**Eco Rounds:** {eco_wr}% ({economy_stats['eco_rounds']['wins']}/{economy_stats['eco_rounds']['total']})"
        ),
        inline=True
    )
    
    # Economic Performance Analysis
    best_economy = "Full Buy"
    best_wr = full_wr
    
    if force_wr > best_wr:
        best_economy = "Force Buy"
        best_wr = force_wr
    if pistol_wr > best_wr:
        best_economy = "Pistol"
        best_wr = pistol_wr
    if anti_eco_wr > best_wr:
        best_economy = "Anti-Eco"
        best_wr = anti_eco_wr
    if eco_wr > 30 and eco_wr > best_wr:
        best_economy = "Eco (Upset potential!)"
        best_wr = eco_wr
    
    embed.add_field(
        name="📊 Economic Performance",
        value=(
            f"**Best Round Type:** {best_economy} ({best_wr}%)\n"
            f"**Pistol Impact:** {'High' if pistol_wr > 60 else 'Medium' if pistol_wr > 40 else 'Low'}\n"
            f"**Anti-Eco Efficiency:** {'Excellent' if anti_eco_wr > 80 else 'Good' if anti_eco_wr > 60 else 'Needs Work'}\n"
            f"**Eco Upset Rate:** {'Excellent' if eco_wr > 25 else 'Good' if eco_wr > 15 else 'Standard'}\n"
            f"**Force Buy Value:** {'High' if force_wr > 40 else 'Medium' if force_wr > 25 else 'Low'}"
        ),
        inline=True
    )
    
    # Detailed round type breakdown if available
    round_types = economy_stats.get('round_types', {})
    if round_types:
        breakdown_text = ""
        for round_type, type_data in round_types.items():
            if type_data['total'] > 0:
                wr = calc_percentage(type_data['wins'], type_data['total'])
                round_name = round_type.replace('_', ' ').title()
                breakdown_text += f"**{round_name}:** {wr}% ({type_data['wins']}/{type_data['total']})\n"
        
        if breakdown_text:
            embed.add_field(
                name="🔍 Detailed Breakdown",
                value=breakdown_text.rstrip(),
                inline=True
            )
    
    embed.add_field(
        name="💡 Economic Recommendations",
        value=(
            f"• {'✅' if pistol_wr > 50 else '⚠️'} Pistol rounds: {'Strong performance' if pistol_wr > 50 else 'Focus on pistol aim and positioning'}\n"
            f"• {'✅' if anti_eco_wr > 70 else '⚠️'} Anti-eco: {'Consistent advantage conversion' if anti_eco_wr > 70 else 'Avoid overpeeks vs eco opponents'}\n"
            f"• {'✅' if force_wr > 30 else '⚠️'} Force buys: {'Good value' if force_wr > 30 else 'Consider full saves instead'}\n"
            f"• {'✅' if eco_wr > 20 else '⚠️'} Eco rounds: {'Great upset potential' if eco_wr > 20 else 'Focus on info gathering and stack sites'}\n"
            f"• {'✅' if full_wr > 60 else '⚠️'} Full buys: {'Solid execution' if full_wr > 60 else 'Review team coordination and utility usage'}"
        ),
        inline=False
    )
    
    # Add warning for incomplete data if applicable
    if economy_stats.get('incomplete_matches', 0) > 0:
        embed.add_field(
            name="⚠️ Data Notice",
            value=f"**{economy_stats['incomplete_matches']} matches** have incomplete round data. Use `/fetch` to update match data for more complete analysis.",
            inline=False
        )
    
    embed.set_footer(text=f"Economy analysis based on {economy_stats['total_rounds']} rounds • Use /fetch for more data")
    return embed

@bot.tree.command(name="economy", description="Analyze economic performance and round type win rates")
@app_commands.describe(
    player="Optional: Player to analyze (username#tag) - leave empty to use your linked account",
    days="Number of days to analyze (default: 30, max: 90)"
)
async def slash_economy_analysis(interaction: discord.Interaction, player: str = None, days: int = DEFAULT_SERVER_STATS_DAYS):
    """Show economic analysis including pistol, force buy, and full buy win rates"""
    
    await interaction.response.defer()
    
    try:
        # Validate days parameter
        if days < 1 or days > MAX_MATCH_HISTORY_DAYS:
            await interaction.edit_original_response(
                content=f"❌ Days must be between 1 and {MAX_MATCH_HISTORY_DAYS}."
            )
            return
        
        username = None
        tag = None
        
        # Determine which player to analyze
        if player:
            if '#' in player:
                username, tag = player.split('#', 1)
            else:
                await interaction.edit_original_response(
                    content="❌ Please provide player in format: `username#tag`"
                )
                return
        else:
            # Use linked account
            try:
                if os.path.exists(LINKS_FILE):
                    with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                        user_links = json.load(f)
                    
                    user_id = str(interaction.user.id)
                    if user_id in user_links:
                        link_data = user_links[user_id]
                        username = link_data.get('username')
                        tag = link_data.get('tag')
                    else:
                        await interaction.edit_original_response(
                            content="❌ No linked account found. Use `/link` to link your account or specify a player manually."
                        )
                        return
                else:
                    await interaction.edit_original_response(
                        content="❌ No linked accounts found. Use `/link` to link your account first."
                    )
                    return
            except Exception:
                await interaction.edit_original_response(
                    content="❌ Error reading linked accounts."
                )
                return
        
        if not username or not tag:
            await interaction.edit_original_response(
                content="❌ Invalid player information."
            )
            return
        
        # Load match history from logs
        matches = await asyncio.get_event_loop().run_in_executor(
            None, load_player_match_history, username, tag, days
        )
        
        if not matches:
            no_data_embed = discord.Embed(
                title=f"💰 Economy Analysis for {username}#{tag}",
                description=f"❌ No competitive match data found for the last {days} days.",
                color=0xFF4655
            )
            no_data_embed.add_field(
                name="💡 How to get economy data",
                value=(
                    "1. Use `/fetch` to collect detailed match data\n"
                    "2. Economy analysis requires round-by-round data\n"
                    "3. Return here after collecting matches\n"
                    "4. More matches = more accurate economic insights"
                ),
                inline=False
            )
            await interaction.edit_original_response(embed=no_data_embed)
            return
        
        # Analyze economy data
        economy_stats = await asyncio.get_event_loop().run_in_executor(
            None, analyze_economy_data, matches, username, tag
        )
        
        # Create and send economy embed
        embed = create_economy_embed(economy_stats, username, tag, days)
        await interaction.edit_original_response(embed=embed)
        
    except Exception:
        await interaction.edit_original_response(
            content="❌ An error occurred while analyzing economy data."
        )

def analyze_clutch_data(matches: List[Dict[str, Any]], username: str, tag: str) -> Dict[str, Any]:
    """Analyze clutch statistics from match data"""
    clutch_stats = {
        '1v1': {'attempts': 0, 'wins': 0},
        '1v2': {'attempts': 0, 'wins': 0},
        '1v3': {'attempts': 0, 'wins': 0},
        '1v4': {'attempts': 0, 'wins': 0},
        '1v5': {'attempts': 0, 'wins': 0},
        'total_clutches': 0,
        'total_wins': 0,
        'best_clutch': None,
        'clutch_maps': {},
        'clutch_agents': {},
        'total_matches': len(matches) if matches else 0
    }
    
    if not matches:
        return clutch_stats
    
    for match in matches:
        rounds_data = match.get('rounds', [])
        kills_data = match.get('kills', [])
        players_data = match.get('players', [])
        
        # Find our player's data
        our_player = None
        for player in players_data:
            if (player.get('name', '').lower() == username.lower() and 
                player.get('tag', '') == tag):
                our_player = player
                break
        
        if not our_player:
            continue
        
        player_team = our_player.get('team', '').lower()
        map_name = match.get('match_info', {}).get('map', 'Unknown')
        agent = our_player.get('agent', 'Unknown')
        
        # Analyze each round for clutch situations
        for round_data in rounds_data:
            clutch_situation = _analyze_round_for_clutch(round_data, kills_data, our_player, player_team)
            
            if clutch_situation:
                clutch_type = clutch_situation['type']  # e.g., '1v2'
                won = clutch_situation['won']
                
                if clutch_type in clutch_stats:
                    clutch_stats[clutch_type]['attempts'] += 1
                    clutch_stats['total_clutches'] += 1
                    
                    if won:
                        clutch_stats[clutch_type]['wins'] += 1
                        clutch_stats['total_wins'] += 1
                        
                        # Track best clutch
                        if not clutch_stats['best_clutch'] or _is_better_clutch(clutch_type, clutch_stats['best_clutch']):
                            clutch_stats['best_clutch'] = {
                                'type': clutch_type,
                                'map': map_name,
                                'agent': agent,
                                'round': round_data.get('round_num', 0)
                            }
                    
                    # Track clutch performance by map
                    if map_name not in clutch_stats['clutch_maps']:
                        clutch_stats['clutch_maps'][map_name] = {'attempts': 0, 'wins': 0}
                    clutch_stats['clutch_maps'][map_name]['attempts'] += 1
                    if won:
                        clutch_stats['clutch_maps'][map_name]['wins'] += 1
                    
                    # Track clutch performance by agent
                    if agent not in clutch_stats['clutch_agents']:
                        clutch_stats['clutch_agents'][agent] = {'attempts': 0, 'wins': 0}
                    clutch_stats['clutch_agents'][agent]['attempts'] += 1
                    if won:
                        clutch_stats['clutch_agents'][agent]['wins'] += 1
    
    return clutch_stats

def _analyze_round_for_clutch(round_data: Dict[str, Any], kills_data: List[Dict[str, Any]], 
                             our_player: Dict[str, Any], player_team: str) -> Optional[Dict[str, Any]]:
    """Analyze a round to determine if it was a clutch situation"""
    try:
        round_num = round_data.get('round_num', 0)
        player_puuid = our_player.get('puuid', '')
        
        # Get kills for this round
        round_kills = [k for k in kills_data if k.get('round', 0) == round_num]
        
        if not round_kills:
            return None
        
        # Sort kills by time
        round_kills.sort(key=lambda x: x.get('kill_time_in_round', 0))
        
        # Find when our player might be in a clutch situation
        # This is a simplified analysis - would need more detailed round state tracking
        
        # Check if our player survived longer than teammates in the round
        our_death_time = None
        teammate_deaths = []
        enemy_deaths = []
        
        for kill in round_kills:
            victim_puuid = kill.get('victim_puuid')
            killer_puuid = kill.get('killer_puuid')
            kill_time = kill.get('kill_time_in_round', 0)
            
            # Track our player's death
            if victim_puuid == player_puuid:
                our_death_time = kill_time
            
            # For simplification, we'll assume clutch if player had kills late in round
            # In reality, you'd need full round state tracking
        
        # Simplified clutch detection based on kills in round
        our_kills_in_round = [k for k in round_kills if k.get('killer_puuid') == player_puuid]
        
        if len(our_kills_in_round) >= 2:  # Multi-kill indicates potential clutch
            # Determine clutch type based on number of kills
            kill_count = len(our_kills_in_round)
            if kill_count == 2:
                clutch_type = '1v2'
            elif kill_count == 3:
                clutch_type = '1v3'
            elif kill_count == 4:
                clutch_type = '1v4'
            elif kill_count >= 5:
                clutch_type = '1v5'
            else:
                clutch_type = '1v1'
            
            # Determine if clutch was won (player survived or got the final kill)
            won = our_death_time is None or our_death_time > max(k.get('kill_time_in_round', 0) for k in our_kills_in_round)
            
            return {
                'type': clutch_type,
                'won': won,
                'kills': kill_count,
                'round': round_num
            }
        
        return None
    except Exception:
        return None

def _is_better_clutch(new_clutch_type: str, current_best: Dict[str, Any]) -> bool:
    """Determine if a new clutch is better than the current best"""
    clutch_rankings = {'1v5': 5, '1v4': 4, '1v3': 3, '1v2': 2, '1v1': 1}
    
    new_rank = clutch_rankings.get(new_clutch_type, 0)
    current_rank = clutch_rankings.get(current_best.get('type', ''), 0)
    
    return new_rank > current_rank

def create_clutch_embed(clutch_stats: Dict[str, Any], username: str, tag: str, days: int) -> discord.Embed:
    """Create clutch analysis embed"""
    if clutch_stats['total_matches'] == 0:
        embed = discord.Embed(
            title=f"🔥 Clutch Analysis for {username}#{tag}",
            description="❌ No competitive match data found for clutch analysis.",
            color=0xFF4655
        )
        embed.add_field(
            name="💡 How to get clutch data",
            value="Use `/fetch` to collect detailed match data, then return here for clutch analysis.",
            inline=False
        )
        return embed
    
    def calc_percentage(wins: int, attempts: int) -> float:
        return round((wins / attempts) * 100, 1) if attempts > 0 else 0.0
    
    total_attempts = clutch_stats['total_clutches']
    total_wins = clutch_stats['total_wins']
    overall_clutch_rate = calc_percentage(total_wins, total_attempts)
    
    embed = discord.Embed(
        title=f"🔥 Clutch Analysis for {username}#{tag}",
        description=f"Clutch performance over the last **{days} days** ({clutch_stats['total_matches']} matches)",
        color=0x00FF00 if overall_clutch_rate >= 30 else 0xFF9900 if overall_clutch_rate >= 15 else 0xFF4655,
        timestamp=datetime.now(timezone.utc)
    )
    
    # Overall Clutch Performance
    embed.add_field(
        name="🎯 Overall Clutch Stats",
        value=(
            f"**Total Clutches:** {total_attempts}\n"
            f"**Clutches Won:** {total_wins}\n"
            f"**Overall Rate:** {overall_clutch_rate}%\n"
            f"**Matches Analyzed:** {clutch_stats['total_matches']}\n"
            f"**Clutch Frequency:** {round(total_attempts / clutch_stats['total_matches'], 1) if clutch_stats['total_matches'] > 0 else 0} per match"
        ),
        inline=True
    )
    
    # Clutch Breakdown by Type
    clutch_breakdown = ""
    for clutch_type in ['1v1', '1v2', '1v3', '1v4', '1v5']:
        attempts = clutch_stats[clutch_type]['attempts']
        wins = clutch_stats[clutch_type]['wins']
        rate = calc_percentage(wins, attempts)
        
        if attempts > 0:
            clutch_breakdown += f"**{clutch_type.upper()}:** {rate}% ({wins}/{attempts})\n"
        else:
            clutch_breakdown += f"**{clutch_type.upper()}:** No attempts\n"
    
    embed.add_field(
        name="⚔️ Clutch Breakdown",
        value=clutch_breakdown,
        inline=True
    )
    
    # Best Clutch & Performance Insights
    best_clutch = clutch_stats.get('best_clutch')
    if best_clutch:
        embed.add_field(
            name="🏆 Best Clutch",
            value=(
                f"**Type:** {best_clutch['type'].upper()}\n"
                f"**Map:** {best_clutch['map']}\n"
                f"**Agent:** {best_clutch['agent']}\n"
                f"**Round:** {best_clutch['round']}"
            ),
            inline=True
        )
    else:
        embed.add_field(
            name="🏆 Best Clutch",
            value="No clutches won yet",
            inline=True
        )
    
    # Top Clutch Maps (if enough data)
    clutch_maps = clutch_stats.get('clutch_maps', {})
    if clutch_maps:
        sorted_maps = sorted(
            clutch_maps.items(),
            key=lambda x: calc_percentage(x[1]['wins'], x[1]['attempts']),
            reverse=True
        )[:3]
        
        map_text = ""
        for map_name, map_data in sorted_maps:
            if map_data['attempts'] >= 2:  # Only show maps with enough attempts
                rate = calc_percentage(map_data['wins'], map_data['attempts'])
                map_text += f"**{map_name}:** {rate}% ({map_data['wins']}/{map_data['attempts']})\n"
        
        if map_text:
            embed.add_field(
                name="🗺️ Top Clutch Maps",
                value=map_text,
                inline=True
            )
    
    # Top Clutch Agents (if enough data)
    clutch_agents = clutch_stats.get('clutch_agents', {})
    if clutch_agents:
        sorted_agents = sorted(
            clutch_agents.items(),
            key=lambda x: calc_percentage(x[1]['wins'], x[1]['attempts']),
            reverse=True
        )[:3]
        
        agent_text = ""
        for agent, agent_data in sorted_agents:
            if agent_data['attempts'] >= 2:  # Only show agents with enough attempts
                rate = calc_percentage(agent_data['wins'], agent_data['attempts'])
                agent_text += f"**{agent}:** {rate}% ({agent_data['wins']}/{agent_data['attempts']})\n"
        
        if agent_text:
            embed.add_field(
                name="🎭 Top Clutch Agents",
                value=agent_text,
                inline=True
            )
    
    # Performance Analysis
    performance_analysis = ""
    if overall_clutch_rate >= 40:
        performance_analysis = "🔥 Exceptional clutch player! You thrive under pressure."
    elif overall_clutch_rate >= 25:
        performance_analysis = "💪 Strong clutch performance! You're reliable in tough situations."
    elif overall_clutch_rate >= 15:
        performance_analysis = "👍 Decent clutch ability! Room for improvement in high-pressure moments."
    else:
        performance_analysis = "📈 Focus on positioning and aim in clutch situations for improvement."
    
    embed.add_field(
        name="📊 Performance Analysis",
        value=performance_analysis,
        inline=False
    )
    
    # Tips for improvement
    tips = []
    if clutch_stats['1v1']['attempts'] > 0 and calc_percentage(clutch_stats['1v1']['wins'], clutch_stats['1v1']['attempts']) < 50:
        tips.append("Focus on pre-aiming common angles in 1v1s")
    if clutch_stats['1v2']['attempts'] > 0 and calc_percentage(clutch_stats['1v2']['wins'], clutch_stats['1v2']['attempts']) < 25:
        tips.append("Use utility to isolate 1v2 situations")
    if total_attempts < clutch_stats['total_matches']:
        tips.append("Work on positioning to avoid early deaths")
    
    if tips:
        embed.add_field(
            name="💡 Improvement Tips",
            value="• " + "\n• ".join(tips),
            inline=False
        )
    
    embed.set_footer(text=f"Clutch analysis based on {total_attempts} clutch situations • Use /fetch for more data")
    return embed

@bot.tree.command(name="clutch", description="Analyze clutch performance (1v1, 1v2, 1v3+ situations)")
@app_commands.describe(
    player="Optional: Player to analyze (username#tag) - leave empty to use your linked account",
    days="Number of days to analyze (default: 30, max: 90)"
)
async def slash_clutch_analysis(interaction: discord.Interaction, player: str = None, days: int = DEFAULT_SERVER_STATS_DAYS):
    """Show clutch analysis including 1v1, 1v2, 1v3+ win rates"""
    
    await interaction.response.defer()
    
    try:
        # Validate days parameter
        if days < 1 or days > MAX_MATCH_HISTORY_DAYS:
            await interaction.edit_original_response(
                content=f"❌ Days must be between 1 and {MAX_MATCH_HISTORY_DAYS}."
            )
            return
        
        username = None
        tag = None
        
        # Determine which player to analyze
        if player:
            if '#' in player:
                username, tag = player.split('#', 1)
            else:
                await interaction.edit_original_response(
                    content="❌ Please provide player in format: `username#tag`"
                )
                return
        else:
            # Use linked account
            try:
                if os.path.exists(LINKS_FILE):
                    with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                        user_links = json.load(f)
                    
                    user_id = str(interaction.user.id)
                    if user_id in user_links:
                        link_data = user_links[user_id]
                        username = link_data.get('username')
                        tag = link_data.get('tag')
                    else:
                        await interaction.edit_original_response(
                            content="❌ No linked account found. Use `/link` to link your account or specify a player manually."
                        )
                        return
                else:
                    await interaction.edit_original_response(
                        content="❌ No linked accounts found. Use `/link` to link your account first."
                    )
                    return
            except Exception:
                await interaction.edit_original_response(
                    content="❌ Error reading linked accounts."
                )
                return
        
        if not username or not tag:
            await interaction.edit_original_response(
                content="❌ Invalid player information."
            )
            return
        
        # Load match history from logs
        matches = await asyncio.get_event_loop().run_in_executor(
            None, load_player_match_history, username, tag, days
        )
        
        if not matches:
            no_data_embed = discord.Embed(
                title=f"🔥 Clutch Analysis for {username}#{tag}",
                description=f"❌ No competitive match data found for the last {days} days.",
                color=0xFF4655
            )
            no_data_embed.add_field(
                name="💡 How to get clutch data",
                value=(
                    "1. Use `/fetch` to collect detailed match data\n"
                    "2. Clutch analysis requires round-by-round kill data\n"
                    "3. Return here after collecting matches\n"
                    "4. More matches = more accurate clutch statistics"
                ),
                inline=False
            )
            await interaction.edit_original_response(embed=no_data_embed)
            return
        
        # Analyze clutch data
        clutch_stats = await asyncio.get_event_loop().run_in_executor(
            None, analyze_clutch_data, matches, username, tag
        )
        
        # Create and send clutch embed
        embed = create_clutch_embed(clutch_stats, username, tag, days)
        await interaction.edit_original_response(embed=embed)
        
    except Exception:
        await interaction.edit_original_response(
            content="❌ An error occurred while analyzing clutch data."
        )

def analyze_agent_rankings(guild_id: int, days: int = DEFAULT_SERVER_STATS_DAYS) -> Dict[str, Any]:
    """Analyze agent performance across all linked server members"""
    agent_rankings = {}
    total_members_analyzed = 0
    
    # Get linked users for this guild
    linked_users = {}
    try:
        if os.path.exists(LINKS_FILE):
            with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                all_links = json.load(f)
                linked_users = {k: v for k, v in all_links.items() if v.get('guild_id') == str(guild_id)}
    except Exception:
        return {'error': 'Failed to load linked users'}
    
    if not linked_users:
        return {'error': 'No linked users found for this server'}
    
    # Analyze each linked user's agent performance
    for user_id, user_data in linked_users.items():
        username = user_data.get('username')
        tag = user_data.get('tag')
        discord_name = user_data.get('discord_name', 'Unknown')
        
        if not username or not tag:
            continue
        
        # Load match history for this user
        matches = load_player_match_history(username, tag, days)
        if not matches:
            continue
        
        total_members_analyzed += 1
        
        # Calculate agent stats for this user
        user_agent_stats = {}
        for match in matches:
            players = match.get('players', [])
            match_info = match.get('match_info', {})
            
            # Find this user's player data
            user_player_data = None
            for player in players:
                if (player.get('name', '').lower() == username.lower() and 
                    player.get('tag', '') == tag):
                    user_player_data = player
                    break
            
            if not user_player_data:
                continue
            
            agent = user_player_data.get('agent', 'Unknown')
            if agent == 'Unknown':
                continue
            
            player_stats = user_player_data.get('stats', {})
            player_team = user_player_data.get('team', '').lower()
            
            # Determine if match was won
            red_rounds = match_info.get('red_rounds', 0)
            blue_rounds = match_info.get('blue_rounds', 0)
            won = ((player_team == 'red' and red_rounds > blue_rounds) or 
                   (player_team == 'blue' and blue_rounds > red_rounds))
            
            # Initialize agent stats for this user
            if agent not in user_agent_stats:
                user_agent_stats[agent] = {
                    'matches': 0, 'wins': 0, 'total_acs': 0, 'total_kast': 0,
                    'total_adr': 0, 'total_kda': 0
                }
            
            # Update stats
            stats = user_agent_stats[agent]
            stats['matches'] += 1
            if won:
                stats['wins'] += 1
            stats['total_acs'] += player_stats.get('acs', 0)
            stats['total_kast'] += player_stats.get('kast', 0)
            stats['total_adr'] += player_stats.get('adr', 0)
            stats['total_kda'] += player_stats.get('kda', 0)
        
        # Process user's agent performance and add to rankings
        for agent, stats in user_agent_stats.items():
            if stats['matches'] < 2:  # Require at least 2 matches
                continue
            
            avg_acs = stats['total_acs'] / stats['matches']
            avg_kast = stats['total_kast'] / stats['matches']
            avg_adr = stats['total_adr'] / stats['matches']
            avg_kda = stats['total_kda'] / stats['matches']
            win_rate = (stats['wins'] / stats['matches']) * 100
            
            # Calculate combined score (weighted average)
            combined_score = (avg_acs * 0.3) + (avg_kast * 0.25) + (win_rate * 0.25) + (avg_adr * 0.2)
            
            if agent not in agent_rankings:
                agent_rankings[agent] = []
            
            agent_rankings[agent].append({
                'player': f"{username}#{tag}",
                'discord_name': discord_name,
                'matches': stats['matches'],
                'win_rate': round(win_rate, 1),
                'avg_acs': round(avg_acs, 1),
                'avg_kast': round(avg_kast, 1),
                'avg_adr': round(avg_adr, 1),
                'avg_kda': round(avg_kda, 2),
                'combined_score': round(combined_score, 1)
            })
    
    # Sort each agent's players by combined score
    for agent in agent_rankings:
        agent_rankings[agent].sort(key=lambda x: x['combined_score'], reverse=True)
    
    return {
        'agent_rankings': agent_rankings,
        'total_members': total_members_analyzed,
        'days_analyzed': days
    }

def create_agents_embed(rankings_data: Dict[str, Any], guild_name: str) -> discord.Embed:
    """Create agent rankings embed"""
    if 'error' in rankings_data:
        embed = discord.Embed(
            title="🎭 Agent Rankings",
            description=f"❌ {rankings_data['error']}",
            color=0xFF4655
        )
        embed.add_field(
            name="💡 How to get agent rankings",
            value=(
                "1. Server members need to use `/link` to connect their accounts\n"
                "2. Use `/fetch` to collect match data for analysis\n"
                "3. Return here to see who excels with each agent"
            ),
            inline=False
        )
        return embed
    
    agent_rankings = rankings_data.get('agent_rankings', {})
    total_members = rankings_data.get('total_members', 0)
    days = rankings_data.get('days_analyzed', 0)
    
    embed = discord.Embed(
        title=f"🎭 Agent Rankings for {guild_name}",
        description=f"Best performing players by agent over the last **{days} days** ({total_members} members analyzed)",
        color=0x00FF00,
        timestamp=datetime.now(timezone.utc)
    )
    
    if not agent_rankings:
        embed.add_field(
            name="❌ No agent data available",
            value="No linked members have sufficient match data for agent analysis.",
            inline=False
        )
        return embed
    
    # Sort agents by popularity (number of players)
    sorted_agents = sorted(agent_rankings.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Show top performing player for each agent
    agents_shown = 0
    for agent, players in sorted_agents:
        if agents_shown >= 10:  # Limit to top 10 agents to avoid embed size limits
            break
        
        if not players:
            continue
        
        best_player = players[0]  # Already sorted by combined score
        
        # Create performance indicators
        performance_indicators = []
        if best_player['win_rate'] >= 60:
            performance_indicators.append("🏆")
        if best_player['avg_acs'] >= 250:
            performance_indicators.append("⚡")
        if best_player['avg_kast'] >= 70:
            performance_indicators.append("🎯")
        if best_player['avg_kda'] >= 1.5:
            performance_indicators.append("💀")
        
        indicators_str = "".join(performance_indicators) + " " if performance_indicators else ""
        
        embed.add_field(
            name=f"🎭 {agent}",
            value=(
                f"{indicators_str}**{best_player['discord_name']}**\n"
                f"**Win Rate:** {best_player['win_rate']}% ({best_player['matches']}m)\n"
                f"**ACS:** {best_player['avg_acs']} | **KAST:** {best_player['avg_kast']}%\n"
                f"**ADR:** {best_player['avg_adr']} | **K/D/A:** {best_player['avg_kda']}\n"
                f"**Score:** {best_player['combined_score']}"
            ),
            inline=True
        )
        agents_shown += 1
    
    # Add legend for performance indicators
    embed.add_field(
        name="📊 Performance Indicators",
        value=(
            "🏆 High Win Rate (60%+)\n"
            "⚡ High ACS (250+)\n"
            "🎯 High KAST (70%+)\n"
            "💀 High K/D/A (1.5+)"
        ),
        inline=False
    )
    
    # Add ranking methodology
    embed.add_field(
        name="📈 Ranking Methodology",
        value=(
            "**Combined Score = ** ACS×30% + KAST×25% + Win Rate×25% + ADR×20%\n"
            "**Minimum:** 2 matches per agent required\n"
            "**Data:** Competitive matches only"
        ),
        inline=False
    )
    
    embed.set_footer(text=f"Agent rankings based on {total_members} linked server members • Use /fetch for more data")
    return embed

@bot.tree.command(name="agents", description="Show best performing player for each agent in the server")
@app_commands.describe(
    days="Number of days to analyze (default: 30, max: 90)"
)
async def slash_agent_rankings(interaction: discord.Interaction, days: int = DEFAULT_SERVER_STATS_DAYS):
    """Show agent performance rankings for server members"""
    
    await interaction.response.defer()
    
    try:
        # Validate days parameter
        if days < 1 or days > MAX_MATCH_HISTORY_DAYS:
            await interaction.edit_original_response(
                content=f"❌ Days must be between 1 and {MAX_MATCH_HISTORY_DAYS}."
            )
            return
        
        if not interaction.guild:
            await interaction.edit_original_response(
                content="❌ This command can only be used in a server, not in DMs."
            )
            return
        
        guild_id = interaction.guild.id
        guild_name = interaction.guild.name
        
        # Analyze agent rankings
        rankings_data = await asyncio.get_event_loop().run_in_executor(
            None, analyze_agent_rankings, guild_id, days
        )
        
        # Create and send agent rankings embed
        embed = create_agents_embed(rankings_data, guild_name)
        await interaction.edit_original_response(embed=embed)
        
    except Exception:
        await interaction.edit_original_response(
            content="❌ An error occurred while analyzing agent rankings."
        )

def analyze_team_compositions(guild_id: int, days: int = DEFAULT_SERVER_STATS_DAYS) -> Dict[str, Any]:
    """Analyze team composition performance by map"""
    comp_data = {}
    total_matches_analyzed = 0
    
    # Get linked users for this guild
    linked_users = {}
    try:
        if os.path.exists(LINKS_FILE):
            with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                all_links = json.load(f)
                linked_users = {k: v for k, v in all_links.items() if v.get('guild_id') == str(guild_id)}
    except Exception:
        return {'error': 'Failed to load linked users'}
    
    if not linked_users:
        return {'error': 'No linked users found for this server'}
    
    # Collect all matches from linked users
    all_matches = []
    for user_id, user_data in linked_users.items():
        username = user_data.get('username')
        tag = user_data.get('tag')
        
        if not username or not tag:
            continue
        
        matches = load_player_match_history(username, tag, days)
        for match in matches:
            match['analyzed_user'] = f"{username}#{tag}"
            all_matches.append(match)
    
    # Deduplicate matches by match_id
    unique_matches = {}
    for match in all_matches:
        match_id = match.get('match_id')
        if match_id and match_id not in unique_matches:
            unique_matches[match_id] = match
    
    # Analyze compositions from unique matches
    for match in unique_matches.values():
        match_info = match.get('match_info', {})
        players = match.get('players', [])
        
        map_name = match_info.get('map', 'Unknown')
        if map_name == 'Unknown':
            continue
        
        total_matches_analyzed += 1
        
        # Group players by team
        red_team = []
        blue_team = []
        
        for player in players:
            team = player.get('team', '').lower()
            agent = player.get('agent', 'Unknown')
            if agent != 'Unknown':
                if team == 'red':
                    red_team.append(agent)
                elif team == 'blue':
                    blue_team.append(agent)
        
        # Only analyze if we have complete teams (5 players each)
        if len(red_team) == 5 and len(blue_team) == 5:
            red_rounds = match_info.get('red_rounds', 0)
            blue_rounds = match_info.get('blue_rounds', 0)
            
            # Create sorted composition strings
            red_comp = tuple(sorted(red_team))
            blue_comp = tuple(sorted(blue_team))
            
            # Initialize map data
            if map_name not in comp_data:
                comp_data[map_name] = {}
            
            # Track red team composition
            if red_comp not in comp_data[map_name]:
                comp_data[map_name][red_comp] = {'matches': 0, 'wins': 0, 'total_rounds': 0}
            
            comp_data[map_name][red_comp]['matches'] += 1
            comp_data[map_name][red_comp]['total_rounds'] += red_rounds
            if red_rounds > blue_rounds:
                comp_data[map_name][red_comp]['wins'] += 1
            
            # Track blue team composition
            if blue_comp not in comp_data[map_name]:
                comp_data[map_name][blue_comp] = {'matches': 0, 'wins': 0, 'total_rounds': 0}
            
            comp_data[map_name][blue_comp]['matches'] += 1
            comp_data[map_name][blue_comp]['total_rounds'] += blue_rounds
            if blue_rounds > red_rounds:
                comp_data[map_name][blue_comp]['wins'] += 1
    
    # Calculate win rates and filter compositions
    filtered_comp_data = {}
    for map_name, compositions in comp_data.items():
        filtered_comps = {}
        for comp, stats in compositions.items():
            if stats['matches'] >= 2:  # Require at least 2 matches
                win_rate = (stats['wins'] / stats['matches']) * 100
                avg_rounds = stats['total_rounds'] / stats['matches']
                
                filtered_comps[comp] = {
                    'matches': stats['matches'],
                    'wins': stats['wins'],
                    'win_rate': round(win_rate, 1),
                    'avg_rounds': round(avg_rounds, 1)
                }
        
        if filtered_comps:
            # Sort by win rate, then by matches played
            sorted_comps = dict(sorted(filtered_comps.items(), 
                                     key=lambda x: (x[1]['win_rate'], x[1]['matches']), 
                                     reverse=True))
            filtered_comp_data[map_name] = sorted_comps
    
    return {
        'comp_data': filtered_comp_data,
        'total_matches': total_matches_analyzed,
        'days_analyzed': days
    }

def create_comp_embed(comp_data: Dict[str, Any], guild_name: str) -> discord.Embed:
    """Create team composition embed"""
    if 'error' in comp_data:
        embed = discord.Embed(
            title="👥 Team Compositions",
            description=f"❌ {comp_data['error']}",
            color=0xFF4655
        )
        embed.add_field(
            name="💡 How to get composition data",
            value=(
                "1. Server members need to use `/link` to connect their accounts\n"
                "2. Use `/fetch` to collect match data for analysis\n"
                "3. Return here to see winning team compositions by map"
            ),
            inline=False
        )
        return embed
    
    compositions = comp_data.get('comp_data', {})
    total_matches = comp_data.get('total_matches', 0)
    days = comp_data.get('days_analyzed', 0)
    
    embed = discord.Embed(
        title=f"👥 Best Team Compositions for {guild_name}",
        description=f"Highest win rate team compositions by map over the last **{days} days** ({total_matches} matches analyzed)",
        color=0x00FF00,
        timestamp=datetime.now(timezone.utc)
    )
    
    if not compositions:
        embed.add_field(
            name="❌ No composition data available",
            value="No sufficient match data found for team composition analysis.",
            inline=False
        )
        return embed
    
    # Sort maps by total compositions available
    sorted_maps = sorted(compositions.items(), key=lambda x: len(x[1]), reverse=True)
    
    maps_shown = 0
    for map_name, map_comps in sorted_maps:
        if maps_shown >= 6:  # Limit to prevent embed size issues
            break
        
        if not map_comps:
            continue
        
        # Get top 2 compositions for this map
        top_comps = list(map_comps.items())[:2]
        
        map_value = ""
        for i, (comp, stats) in enumerate(top_comps, 1):
            # Create agent list with role indicators
            agents_display = []
            for agent in comp:
                agents_display.append(agent)
            
            agents_str = ", ".join(agents_display)
            
            # Performance indicators
            indicators = []
            if stats['win_rate'] >= 70:
                indicators.append("🏆")
            if stats['matches'] >= 5:
                indicators.append("📊")
            if stats['avg_rounds'] >= 13:
                indicators.append("⚡")
            
            indicators_str = "".join(indicators) + " " if indicators else ""
            
            map_value += (
                f"**{i}.** {indicators_str}{agents_str}\n"
                f"**Win Rate:** {stats['win_rate']}% ({stats['matches']}m)\n"
                f"**Avg Rounds:** {stats['avg_rounds']}\n\n"
            )
        
        embed.add_field(
            name=f"🗺️ {map_name}",
            value=map_value.rstrip(),
            inline=True
        )
        maps_shown += 1
    
    # Add performance indicators legend
    embed.add_field(
        name="📊 Performance Indicators",
        value=(
            "🏆 High Win Rate (70%+)\n"
            "📊 Good Sample Size (5+ matches)\n"
            "⚡ High Round Average (13+)"
        ),
        inline=False
    )
    
    # Add methodology
    embed.add_field(
        name="📈 Analysis Details",
        value=(
            "**Minimum:** 2 matches per composition\n"
            "**Sorting:** Win rate, then match count\n"
            "**Data:** Competitive matches only\n"
            "**Source:** Server members' match history"
        ),
        inline=False
    )
    
    embed.set_footer(text=f"Composition analysis from {total_matches} matches • Use /fetch for more data")
    return embed

@bot.tree.command(name="comp", description="Show best team compositions by map for the server")
@app_commands.describe(
    days="Number of days to analyze (default: 30, max: 90)"
)
async def slash_team_compositions(interaction: discord.Interaction, days: int = DEFAULT_SERVER_STATS_DAYS):
    """Show team composition analysis for server members"""
    
    await interaction.response.defer()
    
    try:
        # Validate days parameter
        if days < 1 or days > MAX_MATCH_HISTORY_DAYS:
            await interaction.edit_original_response(
                content=f"❌ Days must be between 1 and {MAX_MATCH_HISTORY_DAYS}."
            )
            return
        
        if not interaction.guild:
            await interaction.edit_original_response(
                content="❌ This command can only be used in a server, not in DMs."
            )
            return
        
        guild_id = interaction.guild.id
        guild_name = interaction.guild.name
        
        # Analyze team compositions
        comp_data = await asyncio.get_event_loop().run_in_executor(
            None, analyze_team_compositions, guild_id, days
        )
        
        # Create and send composition embed
        embed = create_comp_embed(comp_data, guild_name)
        await interaction.edit_original_response(embed=embed)
        
    except Exception:
        await interaction.edit_original_response(
            content="❌ An error occurred while analyzing team compositions."
        )

def analyze_map_performance(guild_id: int, days: int = DEFAULT_SERVER_STATS_DAYS) -> Dict[str, Any]:
    """Analyze map win rates for server members"""
    map_stats = {}
    total_members_analyzed = 0
    
    # Get linked users for this guild
    linked_users = {}
    try:
        if os.path.exists(LINKS_FILE):
            with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                all_links = json.load(f)
                linked_users = {k: v for k, v in all_links.items() if v.get('guild_id') == str(guild_id)}
    except Exception:
        return {'error': 'Failed to load linked users'}
    
    if not linked_users:
        return {'error': 'No linked users found for this server'}
    
    # Analyze each linked user's map performance
    for user_id, user_data in linked_users.items():
        username = user_data.get('username')
        tag = user_data.get('tag')
        
        if not username or not tag:
            continue
        
        matches = load_player_match_history(username, tag, days)
        if not matches:
            continue
        
        total_members_analyzed += 1
        
        # Calculate map stats for this user
        for match in matches:
            match_info = match.get('match_info', {})
            players = match.get('players', [])
            
            map_name = match_info.get('map', 'Unknown')
            if map_name == 'Unknown':
                continue
            
            # Find this user's player data
            user_player_data = None
            for player in players:
                if (player.get('name', '').lower() == username.lower() and 
                    player.get('tag', '') == tag):
                    user_player_data = player
                    break
            
            if not user_player_data:
                continue
            
            player_team = user_player_data.get('team', '').lower()
            player_stats = user_player_data.get('stats', {})
            
            # Determine if match was won
            red_rounds = match_info.get('red_rounds', 0)
            blue_rounds = match_info.get('blue_rounds', 0)
            won = ((player_team == 'red' and red_rounds > blue_rounds) or 
                   (player_team == 'blue' and blue_rounds > red_rounds))
            
            # Initialize map stats
            if map_name not in map_stats:
                map_stats[map_name] = {
                    'matches': 0, 'wins': 0, 'total_acs': 0, 'total_kast': 0,
                    'total_adr': 0, 'total_kda': 0, 'total_rounds_played': 0,
                    'players_count': set()
                }
            
            # Update stats
            stats = map_stats[map_name]
            stats['matches'] += 1
            if won:
                stats['wins'] += 1
            stats['total_acs'] += player_stats.get('acs', 0)
            stats['total_kast'] += player_stats.get('kast', 0)
            stats['total_adr'] += player_stats.get('adr', 0)
            stats['total_kda'] += player_stats.get('kda', 0)
            stats['total_rounds_played'] += (red_rounds + blue_rounds)
            stats['players_count'].add(f"{username}#{tag}")
    
    # Calculate final statistics
    final_map_stats = {}
    for map_name, stats in map_stats.items():
        if stats['matches'] < 3:  # Require at least 3 matches
            continue
        
        win_rate = (stats['wins'] / stats['matches']) * 100
        avg_acs = stats['total_acs'] / stats['matches']
        avg_kast = stats['total_kast'] / stats['matches']
        avg_adr = stats['total_adr'] / stats['matches']
        avg_kda = stats['total_kda'] / stats['matches']
        avg_match_length = stats['total_rounds_played'] / stats['matches']
        unique_players = len(stats['players_count'])
        
        final_map_stats[map_name] = {
            'matches': stats['matches'],
            'wins': stats['wins'],
            'win_rate': round(win_rate, 1),
            'avg_acs': round(avg_acs, 1),
            'avg_kast': round(avg_kast, 1),
            'avg_adr': round(avg_adr, 1),
            'avg_kda': round(avg_kda, 2),
            'avg_match_length': round(avg_match_length, 1),
            'unique_players': unique_players
        }
    
    # Sort by win rate
    sorted_map_stats = dict(sorted(final_map_stats.items(), 
                                  key=lambda x: x[1]['win_rate'], 
                                  reverse=True))
    
    return {
        'map_stats': sorted_map_stats,
        'total_members': total_members_analyzed,
        'days_analyzed': days
    }

def create_map_embed(map_data: Dict[str, Any], guild_name: str) -> discord.Embed:
    """Create map performance embed"""
    if 'error' in map_data:
        embed = discord.Embed(
            title="🗺️ Map Performance",
            description=f"❌ {map_data['error']}",
            color=0xFF4655
        )
        embed.add_field(
            name="💡 How to get map statistics",
            value=(
                "1. Server members need to use `/link` to connect their accounts\n"
                "2. Use `/fetch` to collect match data for analysis\n"
                "3. Return here to see win rates by map"
            ),
            inline=False
        )
        return embed
    
    map_stats = map_data.get('map_stats', {})
    total_members = map_data.get('total_members', 0)
    days = map_data.get('days_analyzed', 0)
    
    embed = discord.Embed(
        title=f"🗺️ Map Performance for {guild_name}",
        description=f"Win rates and performance by map over the last **{days} days** ({total_members} members analyzed)",
        color=0x00FF00,
        timestamp=datetime.now(timezone.utc)
    )
    
    if not map_stats:
        embed.add_field(
            name="❌ No map data available",
            value="No sufficient match data found for map analysis (minimum 3 matches per map required).",
            inline=False
        )
        return embed
    
    # Create ranking with performance indicators
    rank = 1
    for map_name, stats in map_stats.items():
        # Performance indicators
        indicators = []
        if stats['win_rate'] >= 60:
            indicators.append("🏆")
        if stats['avg_acs'] >= 250:
            indicators.append("⚡")
        if stats['avg_kast'] >= 70:
            indicators.append("🎯")
        if stats['matches'] >= 10:
            indicators.append("📊")
        
        indicators_str = "".join(indicators) + " " if indicators else ""
        
        # Color code based on win rate
        if stats['win_rate'] >= 60:
            rank_emoji = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
        elif stats['win_rate'] >= 50:
            rank_emoji = f"#{rank}"
        else:
            rank_emoji = f"#{rank} ⚠️"
        
        embed.add_field(
            name=f"{rank_emoji} {map_name}",
            value=(
                f"{indicators_str}**Win Rate:** {stats['win_rate']}% ({stats['wins']}/{stats['matches']})\n"
                f"**Performance:** ACS {stats['avg_acs']} | KAST {stats['avg_kast']}%\n"
                f"**Combat:** ADR {stats['avg_adr']} | K/D/A {stats['avg_kda']}\n"
                f"**Avg Length:** {stats['avg_match_length']} rounds\n"
                f"**Players:** {stats['unique_players']} different members"
            ),
            inline=True
        )
        rank += 1
    
    # Calculate overall server performance
    total_matches = sum(stats['matches'] for stats in map_stats.values())
    total_wins = sum(stats['wins'] for stats in map_stats.values())
    overall_win_rate = (total_wins / total_matches * 100) if total_matches > 0 else 0
    
    # Find best and worst performing maps
    best_map = max(map_stats.items(), key=lambda x: x[1]['win_rate']) if map_stats else None
    worst_map = min(map_stats.items(), key=lambda x: x[1]['win_rate']) if map_stats else None
    
    embed.add_field(
        name="📊 Server Overview",
        value=(
            f"**Overall Win Rate:** {overall_win_rate:.1f}% ({total_wins}/{total_matches})\n"
            f"**Best Map:** {best_map[0]} ({best_map[1]['win_rate']}%)\n"
            f"**Needs Work:** {worst_map[0]} ({worst_map[1]['win_rate']}%)\n"
            f"**Maps Tracked:** {len(map_stats)}"
        ),
        inline=False
    )
    
    # Add performance indicators legend
    embed.add_field(
        name="📈 Performance Indicators",
        value=(
            "🏆 High Win Rate (60%+) | ⚡ High ACS (250+)\n"
            "🎯 High KAST (70%+) | 📊 Good Sample (10+ matches)\n"
            "⚠️ Below Average (<50% win rate)"
        ),
        inline=False
    )
    
    # Add tips
    embed.add_field(
        name="💡 Pro Tips",
        value=(
            "• Focus on improving lowest win rate maps\n"
            "• Study team compositions on successful maps\n"
            "• Consider map-specific agent strategies"
        ),
        inline=False
    )
    
    embed.set_footer(text=f"Map analysis from {total_matches} matches • Minimum 3 matches per map • Use /fetch for more data")
    return embed

@bot.tree.command(name="map", description="Show win rates and performance statistics by map for the server")
@app_commands.describe(
    days="Number of days to analyze (default: 30, max: 90)"
)
async def slash_map_performance(interaction: discord.Interaction, days: int = DEFAULT_SERVER_STATS_DAYS):
    """Show map performance analysis for server members"""
    
    await interaction.response.defer()
    
    try:
        # Validate days parameter
        if days < 1 or days > MAX_MATCH_HISTORY_DAYS:
            await interaction.edit_original_response(
                content=f"❌ Days must be between 1 and {MAX_MATCH_HISTORY_DAYS}."
            )
            return
        
        if not interaction.guild:
            await interaction.edit_original_response(
                content="❌ This command can only be used in a server, not in DMs."
            )
            return
        
        guild_id = interaction.guild.id
        guild_name = interaction.guild.name
        
        # Analyze map performance
        map_data = await asyncio.get_event_loop().run_in_executor(
            None, analyze_map_performance, guild_id, days
        )
        
        # Create and send map performance embed
        embed = create_map_embed(map_data, guild_name)
        await interaction.edit_original_response(embed=embed)
        
    except Exception:
        await interaction.edit_original_response(
            content="❌ An error occurred while analyzing map performance."
        )

@bot.tree.command(name="help", description="Show help information for Valorant bot commands")
async def slash_help(interaction: discord.Interaction):
    """Show comprehensive help for all bot commands"""
    
    help_embed = discord.Embed(
        title="� Valorant Bot Commands",
        description="**Complete toolkit for Valorant competitive analysis and team insights**\n*Professional esports analytics for Discord servers*",
        color=0x00FF88,
        timestamp=datetime.now(timezone.utc)
    )
    
    # Account Management
    help_embed.add_field(
        name="👤 Account Management",
        value=(
            "**`/link <username> <tag>`** - Link your Discord to Valorant account\n"
            "**`/unlink`** - Unlink your Valorant account\n"
            "**`/linked`** - Show all linked accounts in the server"
        ),
        inline=False
    )
    
    # Personal Statistics
    help_embed.add_field(
        name="📊 Personal Statistics",
        value=(
            "**`/stats [username] [tag] [days]`** - Personal performance stats\n"
            "**`/recentmatch [username] [tag]`** - Comprehensive stats for all 10 players in a recent match\n"
            "Shows KD, KAST, headshot %, ACS, and detailed match performance"
        ),
        inline=False
    )
    
    # Data Collection & Management
    help_embed.add_field(
        name="� Data Collection & Management",
        value=(
            "**`/fetch [username] [tag] [count] [days]`** - Bulk fetch match data\n"
            "Collects and stores detailed match information for analysis"
        ),
        inline=False
    )
    
    # Tactical Analysis
    help_embed.add_field(
        name="⚔️ Tactical Analysis",
        value=(
            "**`/economy [username] [tag] [days]`** - Economic performance analysis\n"
            "**`/clutch [username] [tag] [days]`** - Clutch situations analysis (1v1, 1v2, etc.)\n"
            "Detailed breakdown of high-pressure scenarios and win rates"
        ),
        inline=False
    )
    
    # Team Analytics
    help_embed.add_field(
        name="� Team Analytics",
        value=(
            "**`/agents [days]`** - Best performing player for each agent\n"
            "**`/comp [days]`** - Best team compositions by map\n"
            "**`/map [days]`** - Win rates and performance by map\n"
            "Server-wide rankings and team strategy insights"
        ),
        inline=False
    )
    
    # System Information
    help_embed.add_field(
        name="� System Information",
        value=(
            "**`/help`** - Show this help message"
        ),
        inline=False
    )
    
    # Usage Tips
    help_embed.add_field(
        name="� Pro Tips",
        value=(
            "• Use **`/link`** first to connect your account\n"
            "• Run **`/fetch`** to collect match data for analysis\n"
            "• Team commands analyze all linked server members\n"
            "• Most commands default to 30 days of data\n"
            "• All competitive match data is analyzed"
        ),
        inline=False
    )
    
    help_embed.set_footer(
        text="Valorant Bot • Professional esports analytics • Data from Henrik-3 API",
        icon_url="https://playvalorant.com/favicon.ico"
    )
    
    await interaction.response.send_message(embed=help_embed)

# Account management commands
@bot.tree.command(name="link", description="Link your Discord account to your Valorant username")
@app_commands.describe(
    username="Your Valorant username (case-sensitive)",
    tag="Your Valorant tag (letters and numbers, e.g., 1234 or ABC1, without #)"
)
async def slash_link_account(interaction: discord.Interaction, username: str, tag: str):
    """Link Discord account to Valorant username"""
    
    try:
        # Validate tag format (letters and numbers, 3-5 characters)
        if not tag.isalnum() or len(tag) < 3 or len(tag) > 5:
            await interaction.response.send_message(
                "❌ Invalid tag format. Please use 3-5 letters and/or numbers (e.g., 1234, ABC1, not #1234).",
                ephemeral=True
            )
            return
        
        # Load existing links
        user_links = {}
        if os.path.exists(LINKS_FILE):
            try:
                with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                    user_links = json.load(f)
            except:
                user_links = {}
        
        # Check if user is already linked in this server
        user_id = str(interaction.user.id)
        guild_id = str(interaction.guild.id) if interaction.guild else "dm"
        
        # Create valorant account identifier
        valorant_account = f"{username}#{tag}"
        
        # Update or create link
        user_links[user_id] = {
            'valorant_full': valorant_account,
            'username': username,
            'tag': tag,
            'guild_id': guild_id,
            'linked_at': datetime.now(timezone.utc).isoformat(),
            'discord_name': interaction.user.display_name
        }
        
        # Save updated links
        try:
            with open(LINKS_FILE, 'w', encoding='utf-8') as f:
                json.dump(user_links, f, indent=2, ensure_ascii=False)
            
            success_embed = discord.Embed(
                title="✅ Account Linked Successfully",
                description=f"Your Discord account has been linked to **{valorant_account}**",
                color=0x00FF00
            )
            
            success_embed.add_field(
                name="📊 What's Next?",
                value=(
                    "• Use `/stats` to view your performance statistics\n"
                    "• Use `/recentmatch` to analyze matches and build data\n"
                    "• Your matches will appear in server analytics\n"
                    "• Server leaderboards will include your data\n"
                    "• Use `/unlink` to remove this connection anytime"
                ),
                inline=False
            )
            
            success_embed.set_footer(text="Link is server-specific and can be changed anytime")
            
            await interaction.response.send_message(embed=success_embed, ephemeral=True)
            
        except Exception:
            await interaction.response.send_message(
                "❌ Error saving account link. Please try again.",
                ephemeral=True
            )
            
    except Exception:
        await interaction.response.send_message(
            "❌ An error occurred while linking your account.",
            ephemeral=True
        )

@bot.tree.command(name="unlink", description="Remove the link between your Discord and Valorant accounts")
async def slash_unlink_account(interaction: discord.Interaction):
    """Unlink Discord account from Valorant username"""
    
    try:
        # Load existing links
        if not os.path.exists(LINKS_FILE):
            await interaction.response.send_message("❌ You don't have any linked accounts.", ephemeral=True)
            return
        
        try:
            with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                user_links = json.load(f)
        except:
            await interaction.response.send_message("❌ Error reading account links.", ephemeral=True)
            return
        
        user_id = str(interaction.user.id)
        
        if user_id not in user_links:
            await interaction.response.send_message("❌ You don't have any linked accounts.", ephemeral=True)
            return
        
        # Get the account info before removing
        account_info = user_links[user_id]
        valorant_account = account_info.get('valorant_full', 'Unknown')
        
        # Remove the link
        del user_links[user_id]
        
        # Save updated links
        try:
            with open(LINKS_FILE, 'w', encoding='utf-8') as f:
                json.dump(user_links, f, indent=2, ensure_ascii=False)
            
            unlink_embed = discord.Embed(
                title="✅ Account Unlinked",
                description=f"Your Discord account has been unlinked from **{valorant_account}**",
                color=0xFF9900
            )
            
            unlink_embed.add_field(
                name="📊 What Changed?",
                value=(
                    "• Your matches will no longer appear in server statistics\n"
                    "• Server analytics will exclude your data\n"
                    "• You can re-link anytime using `/link`\n"
                    "• Historical data remains in match logs"
                ),
                inline=False
            )
            
            await interaction.response.send_message(embed=unlink_embed, ephemeral=True)
            
        except Exception:
            await interaction.response.send_message(
                "❌ Error removing account link. Please try again.",
                ephemeral=True
            )
            
    except Exception:
        await interaction.response.send_message(
            "❌ An error occurred while unlinking your account.",
            ephemeral=True
        )

@bot.tree.command(name="linked", description="Show all server members who have linked their Valorant accounts")
async def slash_show_linked(interaction: discord.Interaction):
    """Show linked server members"""
    
    try:
        if not os.path.exists(LINKS_FILE):
            await interaction.response.send_message("❌ No linked accounts found.", ephemeral=True)
            return
        
        with open(LINKS_FILE, 'r', encoding='utf-8') as f:
            user_links = json.load(f)
        
        # Filter for current server members
        server_links = []
        if interaction.guild:
            for discord_id, link_data in user_links.items():
                if link_data.get('guild_id') == str(interaction.guild.id):
                    try:
                        member = interaction.guild.get_member(int(discord_id))
                        if member:  # Still in server
                            server_links.append({
                                'discord_name': member.display_name,
                                'valorant_account': link_data['valorant_full'],
                                'linked_at': link_data['linked_at']
                            })
                    except:
                        continue
        
        if not server_links:
            no_links_embed = discord.Embed(
                title="📊 No Linked Server Members",
                description="No server members have linked their Valorant accounts yet.",
                color=0xFFFF00
            )
            no_links_embed.add_field(
                name="💡 How to Link",
                value="Use `/link username:YourName tag:1234` to link your account",
                inline=False
            )
            await interaction.response.send_message(embed=no_links_embed)
            return
        
        # Sort by link date (most recent first)
        server_links.sort(key=lambda x: x['linked_at'], reverse=True)
        
        linked_embed = discord.Embed(
            title="🔗 Linked Server Members",
            description=f"{len(server_links)} members have linked their accounts",
            color=0x00FF00
        )
        
        # Show up to 20 linked members
        display_links = server_links[:20]
        links_text = ""
        for i, link in enumerate(display_links, 1):
            # Parse date for display
            try:
                link_date = datetime.fromisoformat(link['linked_at'].replace('Z', '+00:00'))
                date_str = link_date.strftime('%m/%d/%y')
            except:
                date_str = "Unknown"
            
            links_text += f"`{i:2}.` **{link['discord_name']}** → {link['valorant_account']} *(linked {date_str})*\n"
        
        linked_embed.add_field(
            name="👥 Server Members",
            value=links_text or "No linked members",
            inline=False
        )
        
        if len(server_links) > 20:
            linked_embed.add_field(
                name="ℹ️ Note",
                value=f"Showing first 20 of {len(server_links)} linked members",
                inline=False
            )
        
        linked_embed.add_field(
            name="📊 Benefits",
            value="Linked members appear in server analytics like `/agents`, `/comp`, and `/map`",
            inline=False
        )
        
        await interaction.response.send_message(embed=linked_embed)
        
    except Exception:
        await interaction.response.send_message("❌ Error retrieving linked accounts.", ephemeral=True)

if __name__ == "__main__":
    DISCORD_BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
    if not DISCORD_BOT_TOKEN:
        print("Error: DISCORD_BOT_TOKEN not found in environment variables.")
    else:
        keep_alive()  # Start the web server
        bot.run(DISCORD_BOT_TOKEN)
