import discord
from discord.ext import commands
from discord import app_commands
import requests
import asyncio
from datetime import datetime, timedelta, timezone
import logging
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)

# Bot setup with intents
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Valorant API base URL
VALORANT_API_BASE = "https://api.henrikdev.xyz/valorant/v1"

# Get API key from environment (optional for basic usage)
HENRIK_API_KEY = os.getenv('HENRIK_API_KEY')

@bot.event
async def on_ready():
    print("🚀 Starting Valorant Discord Bot...")
    print("📊 Using Henrik-3 Valorant API v4.2.0")
    print("⚡ Using modern slash commands with dropdowns")
    
    if HENRIK_API_KEY:
        print("✅ Henrik API key loaded")
        print("📈 Rate limit: 30-90 requests/minute (depending on key type)")
    else:
        print("⚠️ No Henrik API key found - using public rate limits")
        print("📈 Rate limit: Limited requests/minute")
    
    print("\n🎯 Available Commands:")
    print("• /fullmatch - Get comprehensive stats for all 10 players with agents")
    print("• /stats - View individual player statistics and trends")
    print("• /serverstats - View server-wide statistics (linked members only)")
    print("• /economy - Analyze first blood win rates and effective loadouts")
    print("• /firstblood - Analyze first blood win percentage")
    print("• /pullgames - Pull and log specific number of games for testing")
    print("• /link - Link your Discord account to your Valorant username")
    print("• /unlink - Remove your account link")
    print("• /linked - Show all linked server members")
    print("• /help - Show command help")
    print("• /api_status - Check API status")
    print("• /kastdebug - Debug KAST calculations")
    
    print(f'\n{bot.user} has connected to Discord!')
    print(f'Bot is ready and running!')
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} command(s)")
    except Exception as e:
        print(f"Failed to sync commands: {e}")

def calculate_kda(kills, deaths, assists):
    """Calculate KDA ratio: (Kills + Assists) / Deaths"""
    if deaths == 0:
        return float('inf') if kills + assists > 0 else 0.0
    return round((kills + assists) / deaths, 2)

def calculate_adr(damage_dealt, rounds_played):
    """Calculate Average Damage per Round"""
    if rounds_played == 0:
        return 0
    return round(damage_dealt / rounds_played, 1)

def calculate_hs_percentage(headshots, total_shots):
    """Calculate headshot percentage"""
    if total_shots == 0:
        return 0.0
    return round((headshots / total_shots) * 100, 1)

def calculate_kast(match_data, player_puuid, total_rounds):
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
    debug_info = []
    
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
            
        # Debug info
        debug_info.append({
            'round': round_num,
            'kill': has_kill,
            'assist': has_assist, 
            'survive': survived,
            'trade': was_traded,
            'kast': round_kast
        })
    
    # Calculate percentage based on actual rounds with player data
    actual_rounds = max(rounds_checked, total_rounds)
    kast_percentage = round((kast_rounds / actual_rounds) * 100, 1)
    
    # Log debug info for troubleshooting
    logging.info(f"KAST Debug for {player_puuid[:8]}...")
    logging.info(f"Total rounds: {total_rounds}, Rounds checked: {rounds_checked}, KAST rounds: {kast_rounds}")
    logging.info(f"KAST percentage: {kast_percentage}%")
    
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

def calculate_first_kills_deaths(player_rounds):
    """Calculate first kills and first deaths from round data"""
    first_kills = 0
    first_deaths = 0
    
    if not player_rounds:
        return first_kills, first_deaths
    
    for round_data in player_rounds:
        # This depends on the API structure - might need adjustment
        if round_data.get('first_kill', False):
            first_kills += 1
        if round_data.get('first_death', False):
            first_deaths += 1
    
    return first_kills, first_deaths

def analyze_first_blood_win_rate(all_matches):
    """Analyze team win rate when getting first blood"""
    first_blood_stats = {
        'total_rounds': 0,
        'first_blood_rounds': 0,
        'first_blood_wins': 0,
        'first_blood_losses': 0
    }
    
    try:
        for match in all_matches:
            match_kills = match.get('kills', [])
            match_rounds = match.get('rounds', [])
            
            if not match_kills or not match_rounds:
                continue
            
            # Group kills by round to find first blood
            kills_by_round = {}
            for kill_event in match_kills:
                round_num = kill_event.get('round', 0)
                if round_num not in kills_by_round:
                    kills_by_round[round_num] = []
                kills_by_round[round_num].append(kill_event)
            
            # Analyze each round
            for round_info in match_rounds:
                round_num = round_info.get('round_num', round_info.get('round', 0))
                winning_team = round_info.get('winning_team', '').lower()
                
                if not winning_team or round_num not in kills_by_round:
                    continue
                
                first_blood_stats['total_rounds'] += 1
                
                # Find first kill in the round (earliest timestamp)
                round_kills = kills_by_round[round_num]
                if round_kills:
                    # Sort by kill time to get first blood
                    round_kills.sort(key=lambda x: x.get('kill_time_in_round', 0))
                    first_kill = round_kills[0]
                    
                    # Get killer's team
                    killer_team = first_kill.get('killer_team', '').lower()
                    
                    if killer_team:
                        first_blood_stats['first_blood_rounds'] += 1
                        
                        # Check if first blood team won the round
                        if killer_team == winning_team:
                            first_blood_stats['first_blood_wins'] += 1
                        else:
                            first_blood_stats['first_blood_losses'] += 1
        
        # Calculate win rate
        if first_blood_stats['first_blood_rounds'] > 0:
            win_rate = round((first_blood_stats['first_blood_wins'] / first_blood_stats['first_blood_rounds']) * 100, 1)
            first_blood_stats['win_rate'] = win_rate
        else:
            first_blood_stats['win_rate'] = 0.0
            
    except Exception as e:
        logging.error(f"Error analyzing first blood win rate: {e}")
        
    return first_blood_stats

def analyze_effective_loadouts(all_matches):
    """Analyze most effective loadouts (excluding rounds 1, 2, 13, 14)"""
    loadout_stats = {}
    
    try:
        for match in all_matches:
            match_rounds = match.get('rounds', [])
            
            for round_info in match_rounds:
                round_num = round_info.get('round_num', round_info.get('round', 0))
                
                # Skip pistol rounds and potential overtime pistol rounds
                if round_num in [1, 2, 13, 14]:
                    continue
                
                winning_team = round_info.get('winning_team', '').lower()
                if not winning_team:
                    continue
                
                # Get player stats for the round
                player_stats_list = round_info.get('player_stats', [])
                
                # Group players by team
                team_loadouts = {'red': [], 'blue': []}
                team_economy = {'red': 0, 'blue': 0}
                
                for player_stat in player_stats_list:
                    player_team = player_stat.get('team', '').lower()
                    if player_team not in ['red', 'blue']:
                        continue
                    
                    # Get player's economy info for this round
                    economy_data = player_stat.get('economy', {})
                    loadout_value = economy_data.get('loadout_value', 0)
                    weapon = economy_data.get('weapon', {}).get('name', 'Unknown')
                    armor = economy_data.get('armor', {}).get('name', 'None')
                    
                    team_loadouts[player_team].append({
                        'weapon': weapon,
                        'armor': armor,
                        'loadout_value': loadout_value
                    })
                    team_economy[player_team] += loadout_value
                
                # Create team loadout signature
                for team in ['red', 'blue']:
                    if len(team_loadouts[team]) == 5:  # Full team data
                        # Sort weapons to create consistent signature
                        weapons = sorted([p['weapon'] for p in team_loadouts[team]])
                        armor_count = len([p for p in team_loadouts[team] if p['armor'] != 'None'])
                        total_value = team_economy[team]
                        
                        # Create loadout signature
                        signature = f"{'-'.join(weapons[:3])}|A{armor_count}|${total_value}"
                        
                        if signature not in loadout_stats:
                            loadout_stats[signature] = {
                                'total_rounds': 0,
                                'wins': 0,
                                'total_value': total_value,
                                'win_rate': 0.0,
                                'primary_weapons': weapons[:3],
                                'armor_count': armor_count
                            }
                        
                        loadout_stats[signature]['total_rounds'] += 1
                        if team == winning_team:
                            loadout_stats[signature]['wins'] += 1
        
        # Calculate win rates and find most effective
        for signature, stats in loadout_stats.items():
            if stats['total_rounds'] > 0:
                stats['win_rate'] = round((stats['wins'] / stats['total_rounds']) * 100, 1)
        
        # Filter for loadouts with meaningful sample size (at least 5 rounds)
        meaningful_loadouts = {k: v for k, v in loadout_stats.items() if v['total_rounds'] >= 5}
        
        # Sort by efficiency (win rate / cost ratio)
        if meaningful_loadouts:
            sorted_loadouts = sorted(
                meaningful_loadouts.items(), 
                key=lambda x: (x[1]['win_rate'] / max(x[1]['total_value'], 1)) * 100, 
                reverse=True
            )
            return dict(sorted_loadouts[:10])  # Top 10 most efficient
        
    except Exception as e:
        logging.error(f"Error analyzing effective loadouts: {e}")
    
    return loadout_stats

def log_match_data(match_data, requested_username, requested_tag, region):
    """Log match data to JSON file for statistics tracking"""
    try:
        # Create logs directory if it doesn't exist
        logs_dir = "match_logs"
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Get match metadata
        metadata = match_data.get('metadata', {})
        match_id = metadata.get('matchid', 'unknown')
        map_name = metadata.get('map', 'Unknown')
        mode = metadata.get('mode', 'Unknown')
        started_at = metadata.get('game_start_patched', 'Unknown')
        rounds_played = metadata.get('rounds_played', 0)
        
        # Get team scores
        teams = match_data.get('teams', {})
        red_team = teams.get('red', {}) if isinstance(teams, dict) else {}
        blue_team = teams.get('blue', {}) if isinstance(teams, dict) else {}
        red_rounds = red_team.get('rounds_won', 0) if isinstance(red_team, dict) else 0
        blue_rounds = blue_team.get('rounds_won', 0) if isinstance(blue_team, dict) else 0
        
        # Get all players
        players_data = match_data.get('players', {})
        if isinstance(players_data, dict):
            all_players = players_data.get('all_players', [])
        else:
            all_players = []
        
        # Process player data
        processed_players = []
        match_kills = match_data.get('kills', [])
        round_data = match_data.get('rounds', [])
        
        for player in all_players:
            name = player.get('name', 'Unknown')
            tag_val = player.get('tag', '0000')
            stats = player.get('stats', {})
            
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
            
            # Calculate first kills/deaths
            player_puuid = player.get('puuid', '')
            first_bloods = 0
            first_deaths = 0
            
            if match_kills and player_puuid:
                round_kills = {}
                for kill_event in match_kills:
                    round_num = kill_event.get('round', 0)
                    if round_num not in round_kills:
                        round_kills[round_num] = []
                    round_kills[round_num].append(kill_event)
                
                for round_num, kills_in_round in round_kills.items():
                    kills_in_round.sort(key=lambda x: x.get('kill_time_in_round', 0))
                    if kills_in_round and kills_in_round[0].get('killer_puuid') == player_puuid:
                        first_bloods += 1
                    if kills_in_round and kills_in_round[0].get('victim_puuid') == player_puuid:
                        first_deaths += 1
            
            # Calculate multikills
            total_multikills = 0
            if round_data and player_puuid:
                for round_info in round_data:
                    player_stats_list = round_info.get('player_stats', [])
                    for player_round_stat in player_stats_list:
                        if player_round_stat.get('player_puuid') == player_puuid:
                            round_kills = player_round_stat.get('kills', 0)
                            if round_kills >= 3:
                                total_multikills += 1
                            break
            
            # Calculate KAST percentage
            kast_pct = calculate_kast(match_data, player_puuid, rounds_played)
            
            processed_players.append({
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
                    'kda': kda,
                    'kast': kast_pct,
                    'score': total_score,
                    'damage': damage,
                    'first_bloods': first_bloods,
                    'first_deaths': first_deaths,
                    'multikills': total_multikills,
                    'plus_minus': kills - deaths
                },
                'is_requested_player': (name.lower() == requested_username.lower() and tag_val == requested_tag)
            })
        
        # Create log entry
        log_entry = {
            'timestamp': datetime.now(datetime.UTC if hasattr(datetime, 'UTC') else timezone.utc).isoformat(),
            'match_id': match_id,
            'requested_player': f"{requested_username}#{requested_tag}",
            'region': region,
            'match_info': {
                'map': map_name,
                'mode': mode,
                'started_at': started_at,
                'rounds_played': rounds_played,
                'score': f"{red_rounds}-{blue_rounds}",
                'red_rounds': red_rounds,
                'blue_rounds': blue_rounds
            },
            'players': processed_players
        }
        
        # Append to daily log file
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        log_file = os.path.join(logs_dir, f"matches_{today}.json")
        
        # Read existing data or create new list
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                try:
                    existing_data = json.load(f)
                    if not isinstance(existing_data, list):
                        existing_data = []
                except json.JSONDecodeError:
                    existing_data = []
        else:
            existing_data = []
        
        # Check if match already logged (avoid duplicates)
        match_exists = any(entry.get('match_id') == match_id for entry in existing_data)
        if not match_exists:
            existing_data.append(log_entry)
            
            # Write back to file
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Logged match {match_id} to {log_file}")
            return True  # Match was newly logged
        else:
            logging.info(f"Match {match_id} already logged, skipping duplicate")
            return False  # Match was already logged
            
    except Exception as e:
        logging.error(f"Error logging match data: {e}")
        return False

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
                logging.info(f"Successfully fetched match details for {match_id}")
                return match_data
            else:
                logging.error(f"No data in response for match {match_id}")
                return {}
        elif response.status_code == 404:
            logging.error(f"Match not found: {match_id}")
            return {}
        elif response.status_code == 429:
            logging.error(f"Rate limited while fetching match {match_id}")
            return {}
        else:
            logging.error(f"Failed to fetch match details for {match_id}: {response.status_code} - {response.text}")
            return {}
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching match details for {match_id}: {e}")
        return {}

def create_comprehensive_match_embed(match_data, username, tag, region):
    """Create a Discord embed with comprehensive match information for all players"""
    embed = discord.Embed(
        title=f"🎯 Complete Match Stats for {username}#{tag}",
        description=f"Detailed stats for all players in **{region.upper()}** region",
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
        

        
        # Separate players by team (Henrik API uses team field)
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
                return f"**{team_name} Team:** No data"
            
            stats_text = f"**{team_color} {team_name} Team:**\n```\n"
            # Improved column alignment with consistent widths
            stats_text += f"{'Player':<16} {'Agent':<11} {'K/D/A':<7} {'ACS':<4} {'ADR':<4} {'HS%':<4} {'KAST':<4} {'FK':<2} {'FD':<2} {'MK':<2} {'+/-':<3}\n"
            stats_text += "─" * 80 + "\n"
            
            for player in players:
                name = player.get('name', 'Unknown')
                tag_val = player.get('tag', '0000')
                stats = player.get('stats', {})
                
                # Basic stats
                kills = stats.get('kills', 0)
                deaths = stats.get('deaths', 0)
                assists = stats.get('assists', 0)
                
                # Henrik API v2 correct field structure
                total_score = stats.get('score', 0)
                damage = player.get('damage_made', 0)  # damage_made is at player level, not in stats
                
                # Combat stats
                headshots = stats.get('headshots', 0)
                bodyshots = stats.get('bodyshots', 0)
                legshots = stats.get('legshots', 0)
                total_shots = headshots + bodyshots + legshots
                
                # Calculate ACS correctly: total_score / rounds_played
                acs_display = int(total_score / rounds_played) if rounds_played > 0 else total_score
                
                # Calculate first kills/deaths from match kills data
                match_kills = match_data.get('kills', [])
                first_bloods = 0
                first_deaths = 0
                
                # Get player's PUUID for kill tracking
                player_puuid = player.get('puuid', '')
                
                # Count first kills and first deaths per round
                if match_kills and player_puuid:
                    # Group kills by round to find first kill/death per round
                    round_kills = {}
                    for kill_event in match_kills:
                        round_num = kill_event.get('round', 0)
                        if round_num not in round_kills:
                            round_kills[round_num] = []
                        round_kills[round_num].append(kill_event)
                    
                    # Check each round for first blood and first death
                    for round_num, kills_in_round in round_kills.items():
                        # Sort kills by time to find first kill/death
                        kills_in_round.sort(key=lambda x: x.get('kill_time_in_round', 0))
                        
                        # Check if player got first kill (first blood)
                        if kills_in_round and kills_in_round[0].get('killer_puuid') == player_puuid:
                            first_bloods += 1
                        
                        # Check if player died first
                        if kills_in_round and kills_in_round[0].get('victim_puuid') == player_puuid:
                            first_deaths += 1
                
                # Calculate multikills from round stats
                round_data = match_data.get('rounds', [])
                total_multikills = 0
                
                if round_data and player_puuid:
                    for round_info in round_data:
                        player_stats_list = round_info.get('player_stats', [])
                        for player_round_stat in player_stats_list:
                            if player_round_stat.get('player_puuid') == player_puuid:
                                round_kills = player_round_stat.get('kills', 0)
                                if round_kills >= 3:
                                    total_multikills += 1
                                break
                
                # Calculate KAST percentage for this player
                kast_pct = calculate_kast(match_data, player_puuid, rounds_played)
                
                # Calculate derived metrics
                kda = calculate_kda(kills, deaths, assists)
                adr = calculate_adr(damage, rounds_played) if damage > 0 and rounds_played > 0 else 0
                hs_pct = calculate_hs_percentage(headshots, total_shots)
                plus_minus = kills - deaths
                
                # Format player name (highlight if it's the requested user)
                player_name = f"{name}#{tag_val}"
                if name.lower() == username.lower() and tag_val == tag:
                    player_name = f"►{player_name[:14]}"  # Account for arrow symbol
                else:
                    player_name = player_name[:15]
                
                # Get agent name and truncate if needed
                agent = player.get('character', 'Unknown')
                agent_display = agent[:10] if agent else 'Unknown'
                
                # Format KDA display with consistent width
                kda_str = f"{kills}/{deaths}/{assists}"
                if len(kda_str) > 7:
                    kda_str = kda_str[:7]
                
                # Format numbers consistently
                acs_str = f"{acs_display}"
                adr_str = f"{int(adr)}"
                hs_str = f"{int(hs_pct)}"
                kast_str = f"{int(kast_pct)}"
                plus_minus_str = f"{plus_minus:+d}" if plus_minus != 0 else "0"
                
                # Build the formatted line with proper alignment
                stats_text += f"{player_name:<16} {agent_display:<11} {kda_str:<7} {acs_str:<4} {adr_str:<4} {hs_str:<4} {kast_str:<4} {first_bloods:<2} {first_deaths:<2} {total_multikills:<2} {plus_minus_str:<3}\n"
            
            stats_text += "```"
            return stats_text
        
        # Add team stats
        if red_players:
            red_stats = format_player_stats(red_players, "Red", "🔴")
            embed.add_field(name="🔴 Red Team", value=red_stats, inline=False)
        
        if blue_players:
            blue_stats = format_player_stats(blue_players, "Blue", "🔵")
            embed.add_field(name="🔵 Blue Team", value=blue_stats, inline=False)
        
        # Log match data for statistics tracking
        try:
            log_match_data(match_data, username, tag, region)
        except Exception as log_error:
            logging.error(f"Failed to log match data: {log_error}")
        
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
        
        # Find and highlight requested player's detailed stats
        requested_player = None
        for player in all_players:
            if (player.get('name', '').lower() == username.lower() and 
                player.get('tag', '') == tag):
                requested_player = player
                break
        
        if requested_player:
            stats = requested_player.get('stats', {})
            kills = stats.get('kills', 0)
            deaths = stats.get('deaths', 0)
            assists = stats.get('assists', 0)
            damage = requested_player.get('damage_made', 0)  # damage_made is at player level, not in stats
            
            # Calculate first kills/deaths from match kills data (same logic as in table)
            match_kills = match_data.get('kills', [])
            first_bloods = 0
            first_deaths = 0
            
            # Get player's PUUID for kill tracking
            player_puuid = requested_player.get('puuid', '')
            
            # Count first kills and first deaths per round
            if match_kills and player_puuid:
                # Group kills by round to find first kill/death per round
                round_kills = {}
                for kill_event in match_kills:
                    round_num = kill_event.get('round', 0)
                    if round_num not in round_kills:
                        round_kills[round_num] = []
                    round_kills[round_num].append(kill_event)
                
                # Check each round for first blood and first death
                for round_num, kills_in_round in round_kills.items():
                    # Sort kills by time to find first kill/death
                    kills_in_round.sort(key=lambda x: x.get('kill_time_in_round', 0))
                    
                    # Check if player got first kill (first blood)
                    if kills_in_round and kills_in_round[0].get('killer_puuid') == player_puuid:
                        first_bloods += 1
                    
                    # Check if player died first
                    if kills_in_round and kills_in_round[0].get('victim_puuid') == player_puuid:
                        first_deaths += 1
            
            # Calculate multikills for requested player (same logic as in table)
            round_data = match_data.get('rounds', [])
            multikills = {'2k': 0, '3k': 0, '4k': 0, '5k': 0}
            
            if round_data and player_puuid:
                for round_info in round_data:
                    player_stats_list = round_info.get('player_stats', [])
                    for player_round_stat in player_stats_list:
                        if player_round_stat.get('player_puuid') == player_puuid:
                            round_kills = player_round_stat.get('kills', 0)
                            if round_kills >= 2:
                                if round_kills == 2:
                                    multikills['2k'] += 1
                                elif round_kills == 3:
                                    multikills['3k'] += 1
                                elif round_kills == 4:
                                    multikills['4k'] += 1
                                elif round_kills >= 5:
                                    multikills['5k'] += 1
                            break
            
            # Calculate additional metrics for the requested player
            kda = calculate_kda(kills, deaths, assists)
            adr = calculate_adr(damage, rounds_played)
            
            # Try to get rank info
            rank = requested_player.get('currenttier_patched', 'Unranked')
            
            # Format multikills display
            mk_display = []
            if multikills.get('2k', 0) > 0:
                mk_display.append(f"2K: {multikills['2k']}")
            if multikills.get('3k', 0) > 0:
                mk_display.append(f"3K: {multikills['3k']}")
            if multikills.get('4k', 0) > 0:
                mk_display.append(f"4K: {multikills['4k']}")
            if multikills.get('5k', 0) > 0:
                mk_display.append(f"5K: {multikills['5k']}")
            
            mk_str = ", ".join(mk_display) if mk_display else "None"
            
            embed.add_field(
                name=f"🎯 {username}#{tag} Details",
                value=(
                    f"**KDA Ratio:** {kda}\n"
                    f"**Total Damage:** {damage:,}\n"
                    f"**First Kills:** {first_bloods}\n"
                    f"**First Deaths:** {first_deaths}\n"
                    f"**Multi-kills:** {mk_str}\n"
                    f"**Rank:** {rank}\n"
                    f"**Team:** {requested_player.get('team', 'Unknown').upper()}"
                ),
                inline=True
            )
        
    except Exception as e:
        logging.error(f"Error processing comprehensive match data: {e}")
        embed.add_field(
            name="❌ Error",
            value="Error processing comprehensive match data",
            inline=False
        )
    
    embed.set_footer(text=f"Match ID: {match_data.get('metadata', {}).get('matchid', 'Unknown')} • Henrik-3 API")
    return embed

def fetch_valorant_matches(region, username, tag):
    """Fetch matches from Valorant API"""
    # URL encode the username and tag to handle spaces and special characters
    encoded_username = requests.utils.quote(username, safe='')
    encoded_tag = requests.utils.quote(tag, safe='')
    
    # Use v3 API endpoint as per Henrik API documentation
    url = f"{VALORANT_API_BASE.replace('v1', 'v3')}/matches/{region}/{encoded_username}/{encoded_tag}"
    
    # Set up headers with API key if available
    headers = {
        'User-Agent': 'Valorant-Discord-Bot/1.0'
    }
    
    if HENRIK_API_KEY:
        headers['Authorization'] = HENRIK_API_KEY  # Henrik API uses direct key in Authorization header
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('data', [])
        elif response.status_code == 404:
            logging.info(f"Player not found: {username}#{tag} in {region}")
            return None
        elif response.status_code == 429:
            logging.warning("Rate limited by Henrik API")
            return []
        elif response.status_code == 400:
            logging.error(f"Bad request - Invalid parameters: {username}#{tag} in {region}")
            return []
        elif response.status_code == 403:
            logging.error("API key invalid or insufficient permissions")
            return []
        else:
            logging.error(f"API returned status code: {response.status_code}")
            logging.error(f"Response: {response.text}")
            return []
            
    except requests.exceptions.Timeout:
        logging.error("API request timed out (15s)")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return []

@bot.tree.command(name="fullmatch", description="Get comprehensive stats for all 10 players in a match")
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
    """
    Get comprehensive Valorant match report showing all 10 players' stats
    """
    
    # Validate tag (should be alphanumeric and reasonable length)
    if not tag.isalnum() or len(tag) > 10:
        error_embed = discord.Embed(
            title="❌ Invalid Tag Format",
            description=(
                f"Tag should contain only letters and numbers (max 10 characters)\n"
                f"Examples: `1234`, `ABC1`, `POG`\n"
                f"You provided: `{tag}`"
            ),
            color=0xFF0000
        )
        await interaction.response.send_message(embed=error_embed, ephemeral=True)
        return
    
    # Send initial response (loading message)
    loading_embed = discord.Embed(
        title="🔄 Fetching comprehensive match data...",
        description=f"Getting detailed stats for all players in **{username}#{tag}**'s latest match in **{region.name}**",
        color=0xFFFF00
    )
    await interaction.response.send_message(embed=loading_embed)
    
    try:
        # First fetch the player's recent matches to get the match ID
        matches = await asyncio.get_event_loop().run_in_executor(
            None, fetch_valorant_matches, region.value, username, tag
        )
        
        if matches is None:
            # Player not found
            error_embed = discord.Embed(
                title="❌ Player Not Found",
                description=(
                    f"Could not find player **{username}#{tag}** in **{region.name}**\n\n"
                    "**Possible reasons:**\n"
                    "• Player name or tag is incorrect\n"
                    "• Player hasn't played recently\n"
                    "• Player profile is set to private"
                ),
                color=0xFF0000
            )
            await interaction.edit_original_response(embed=error_embed)
            return
        
        if not matches:
            # No matches or API error
            error_embed = discord.Embed(
                title="❌ No Data Available",
                description=(
                    f"No recent match data found for **{username}#{tag}**\n\n"
                    "**This could be due to:**\n"
                    "• Player has no recent matches\n"
                    "• API is temporarily unavailable\n"
                    "• Rate limit exceeded (try again in a few minutes)\n"
                    "• Player profile is private"
                ),
                color=0xFF0000
            )
            await interaction.edit_original_response(embed=error_embed)
            return
        
        # Get the match ID from the most recent match
        latest_match = matches[0]
        match_id = latest_match.get('metadata', {}).get('matchid')
        
        if not match_id:
            error_embed = discord.Embed(
                title="❌ No Match ID",
                description="Unable to retrieve match ID for detailed stats.",
                color=0xFF0000
            )
            await interaction.edit_original_response(embed=error_embed)
            return
        
        # Fetch detailed match data using the match ID
        match_data = await asyncio.get_event_loop().run_in_executor(
            None, fetch_match_details, match_id
        )
        
        if not match_data:
            error_embed = discord.Embed(
                title="❌ Match Details Unavailable",
                description="Unable to retrieve detailed match statistics.",
                color=0xFF0000
            )
            await interaction.edit_original_response(embed=error_embed)
            return
        
        # Create and send comprehensive match report embed
        comprehensive_embed = create_comprehensive_match_embed(match_data, username, tag, region.value)
        await interaction.edit_original_response(embed=comprehensive_embed)
        
    except Exception as e:
        logging.error(f"Error in slash_full_match_report command: {e}")
        error_embed = discord.Embed(
            title="❌ Error",
            description="An unexpected error occurred while fetching comprehensive match data. Please try again later.",
            color=0xFF0000
        )
        await interaction.edit_original_response(embed=error_embed)

@bot.tree.command(name="help", description="Show help information for Valorant bot commands")
async def slash_help(interaction: discord.Interaction):
    """Show help information for Valorant bot commands"""
    help_embed = discord.Embed(
        title="🎯 Valorant Bot Help",
        description="Get detailed stats for your most recent Valorant match (tracker.gg style)",
        color=0xFF4655
    )
    
    help_embed.add_field(
        name="🎯 Complete Match Analysis",
        value=(
            "**Command:** `/fullmatch`\n"
            "**Description:** Shows detailed stats for ALL 10 players in the most recent match\n"
            "**Stats included:** Agent, K/D/A, ACS, ADR, HS%, KAST%, FK, FD, Multi-kills (3K+), +/-\n"
            "**Features:** Team separation, agent display, player highlighting, comprehensive scoreboard\n"
            "**Parameters:**\n"
            "• `region` - Select from dropdown (EU, NA, AP, KR)\n"
            "• `username` - Player name (case-sensitive, spaces allowed)\n"
            "• `tag` - Player tag (letters/numbers, e.g., 1234, ABC1, POG)"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="🔗 Account Linking",
        value=(
            "**Commands:** `/link`, `/unlink`, `/linked`\n"
            "**Description:** Link your Discord account to your Valorant username\n"
            "**Benefits:** Appear in server-only statistics, leaderboards, and analytics\n"
            "**Usage:** `/link username:YourName tag:1234`\n"
            "**Privacy:** Links are server-specific and can be removed anytime"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="🌐 Server-Wide Statistics",
        value=(
            "**Command:** `/serverstats`\n"
            "**Description:** View comprehensive server-wide analytics from all logged matches\n"
            "**Features:** Match overview, map/agent statistics, top performers, server averages, map win rates, best agents per player, best individual games\n"
            "**Parameters:** `days` (1-90, default: 30), `server_only` (default: True)\n"
            "**Shows:** Most played maps/agents, top ACS/K-D/KAST players, performance trends, win rates per map, player specializations"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="📊 Individual Player Statistics",
        value=(
            "**Command:** `/stats`\n"
            "**Description:** View detailed statistics and trends for a specific player\n"
            "**Features:** Win rate, performance averages, combat stats, KAST%, recent match trends\n"
            "**Parameters:** `username`, `tag`, `days` (1-30, default: 7)\n"
            "**Note:** Requires previous `/fullmatch` usage to build player data"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="� Economy & Tactical Analysis",
        value=(
            "**Command:** `/economy`\n"
            "**Description:** Analyze team economy, first blood impact, and most effective loadouts\n"
            "**Features:** First blood win rates, cost-effective weapon combinations, economy insights\n"
            "**Parameters:** `days` (1-90, default: 30), `server_only` (default: True)\n"
            "**Shows:** First blood advantage, efficient loadouts (excludes pistol rounds), economy metrics"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="🩸 First Blood Analysis",
        value=(
            "**Command:** `/firstblood`\n"
            "**Description:** Dedicated first blood win percentage analysis\n"
            "**Parameters:** `days` (1-90, default: 30), `server_only` (default: True)\n"
            "**Shows:** First blood win rates, tactical advantage, strategic impact analysis"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="📥 Testing & Data Collection",
        value=(
            "**Command:** `/pullgames`\n"
            "**Description:** Pull and log specific number of recent games for testing/analysis\n"
            "**Parameters:** `region`, `username`, `tag`, `count` (1-20, default: 5)\n"
            "**Purpose:** Bulk data collection for testing and analysis"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="�📊 API Status",
        value=(
            "**Command:** `/api_status`\n"
            "**Description:** Shows API status, rate limits, and authentication info"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="🌍 Supported Regions",
        value="Europe (EU), North America (NA), Asia Pacific (AP), Korea (KR)",
        inline=False
    )
    
    help_embed.add_field(
        name="ℹ️ Notes",
        value=(
            "• Player profiles must be public to view match data\n"
            "• `/fullmatch` automatically logs data for statistics tracking\n"
            "• Link your account to appear in server-only statistics\n"
            "• Server stats show only linked members by default\n"
            "• Data is provided by Henrik-3 Valorant API"
        ),
        inline=False
    )
    
    help_embed.add_field(
        name="💡 Examples",
        value=(
            "**Match Analysis:**\n"
            "• `/fullmatch region:Europe (EU) username:TenZ tag:1234`\n"
            "• `/fullmatch region:Asia Pacific (AP) username:tietoa tag:POG`\n\n"
            "**Account Management:**\n"
            "• `/link username:TenZ tag:1234`\n"
            "• `/linked` - See who's linked\n\n"
            "**Statistics & Analysis:**\n"
            "• `/stats username:TenZ tag:1234 days:14`\n"
            "• `/serverstats days:60 server_only:True`\n"
            "• `/economy days:30 server_only:True`\n"
            "• `/firstblood days:14 server_only:True`\n\n"
            "**Testing & Data Collection:**\n"
            "• `/pullgames region:North America (NA) username:TenZ tag:1234 count:10`"
        ),
        inline=False
    )
    
    await interaction.response.send_message(embed=help_embed)

@bot.tree.command(name="api_status", description="Show API status and rate limit information")
async def slash_api_status(interaction: discord.Interaction):
    """Show API status and rate limit information"""
    status_embed = discord.Embed(
        title="📊 API Status",
        description="Henrik-3 Valorant API Information",
        color=0x00FF00
    )
    
    status_embed.add_field(
        name="🔗 API Version",
        value="v4.2.0 (Henrik-3 Unofficial Valorant API)\nMatches: v3 endpoint",
        inline=False
    )
    
    if HENRIK_API_KEY:
        status_embed.add_field(
            name="🔑 Authentication",
            value="✅ API Key Configured",
            inline=True
        )
        status_embed.add_field(
            name="📈 Rate Limit",
            value="30-90 requests/minute\n(depending on key type)",
            inline=True
        )
    else:
        status_embed.add_field(
            name="🔑 Authentication",
            value="❌ No API Key\n(Public rate limits)",
            inline=True
        )
        status_embed.add_field(
            name="📈 Rate Limit",
            value="Limited requests/minute\n(Get a key for more)",
            inline=True
        )
    
    status_embed.add_field(
        name="🌍 Supported Regions",
        value="EU, NA, AP, KR",
        inline=True
    )
    
    status_embed.add_field(
        name="📖 Documentation",
        value="[Henrik API Docs](https://docs.henrikdev.xyz/valorant)",
        inline=False
    )
    
    status_embed.add_field(
        name="🔧 Get API Key",
        value=(
            "1. Join [Henrik Discord](https://discord.com/invite/X3GaVkX2YN)\n"
            "2. Verify your account\n"
            "3. Go to #get-a-key channel\n"
            "4. Select 'VALORANT' from dropdown\n"
            "5. Choose 'Basic' (30 req/min) or 'Advanced' (90 req/min)"
        ),
        inline=False
    )
    
    await interaction.response.send_message(embed=status_embed)

@bot.tree.command(name="stats", description="View player statistics and trends from logged match data")
@app_commands.describe(
    username="Player's username (case-sensitive, spaces allowed)",
    tag="Player's tag (letters and numbers, e.g., 1234 or ABC1)",
    days="Number of days to look back (default: 7, max: 30)"
)
async def slash_stats(interaction: discord.Interaction, username: str, tag: str, days: int = 7):
    """View player statistics from logged match data"""
    
    # Validate inputs
    if not tag.isalnum() or len(tag) > 10:
        error_embed = discord.Embed(
            title="❌ Invalid Tag Format",
            description=f"Tag should contain only letters and numbers (max 10 characters)",
            color=0xFF0000
        )
        await interaction.response.send_message(embed=error_embed, ephemeral=True)
        return
    
    if days < 1 or days > 30:
        days = 7
    
    await interaction.response.send_message("🔍 Analyzing match history...", ephemeral=False)
    
    try:
        # Collect data from log files
        logs_dir = "match_logs"
        player_identifier = f"{username}#{tag}"
        player_matches = []
        
        if not os.path.exists(logs_dir):
            await interaction.edit_original_response(content="❌ No match data found. Play some matches with `/fullmatch` first!")
            return
        
        # Check last N days
        for i in range(days):
            date = (datetime.now(timezone.utc) - timedelta(days=i)).strftime('%Y-%m-%d')
            log_file = os.path.join(logs_dir, f"matches_{date}.json")
            
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        daily_matches = json.load(f)
                        for match in daily_matches:
                            # Find matches where this player was the requested player or was in the match
                            for player in match.get('players', []):
                                if (player.get('name', '').lower() == username.lower() and 
                                    player.get('tag', '') == tag):
                                    player_matches.append({
                                        'match': match,
                                        'player_stats': player
                                    })
                                    break
                except json.JSONDecodeError:
                    continue
        
        if not player_matches:
            await interaction.edit_original_response(content=f"❌ No match data found for **{username}#{tag}** in the last {days} days.")
            return
        
        # Calculate statistics
        total_matches = len(player_matches)
        total_kills = sum(m['player_stats']['stats']['kills'] for m in player_matches)
        total_deaths = sum(m['player_stats']['stats']['deaths'] for m in player_matches)
        total_assists = sum(m['player_stats']['stats']['assists'] for m in player_matches)
        total_damage = sum(m['player_stats']['stats']['damage'] for m in player_matches)
        total_first_bloods = sum(m['player_stats']['stats']['first_bloods'] for m in player_matches)
        total_multikills = sum(m['player_stats']['stats']['multikills'] for m in player_matches)
        total_kast = sum(m['player_stats']['stats'].get('kast', 0) for m in player_matches)
        
        # Calculate averages
        avg_kills = round(total_kills / total_matches, 1)
        avg_deaths = round(total_deaths / total_matches, 1)
        avg_assists = round(total_assists / total_matches, 1)
        avg_acs = round(sum(m['player_stats']['stats']['acs'] for m in player_matches) / total_matches, 1)
        avg_adr = round(sum(m['player_stats']['stats']['adr'] for m in player_matches) / total_matches, 1)
        avg_hs_pct = round(sum(m['player_stats']['stats']['headshot_pct'] for m in player_matches) / total_matches, 1)
        avg_kast = round(total_kast / total_matches, 1)
        overall_kda = round((total_kills + total_assists) / total_deaths, 2) if total_deaths > 0 else float('inf')
        
        # Win rate calculation
        wins = 0
        for match_data in player_matches:
            match_info = match_data['match']['match_info']
            player_team = match_data['player_stats']['team'].lower()
            red_rounds = match_info['red_rounds']
            blue_rounds = match_info['blue_rounds']
            
            if ((player_team == 'red' and red_rounds > blue_rounds) or 
                (player_team == 'blue' and blue_rounds > red_rounds)):
                wins += 1
        
        win_rate = round((wins / total_matches) * 100, 1)
        
        # Most played maps and agents
        maps = {}
        agents = {}
        for match_data in player_matches:
            map_name = match_data['match']['match_info']['map']
            agent = match_data['player_stats'].get('agent', 'Unknown')
            maps[map_name] = maps.get(map_name, 0) + 1
            agents[agent] = agents.get(agent, 0) + 1
        
        most_played_map = max(maps.items(), key=lambda x: x[1]) if maps else ("Unknown", 0)
        most_played_agent = max(agents.items(), key=lambda x: x[1]) if agents else ("Unknown", 0)
        
        # Create statistics embed
        stats_embed = discord.Embed(
            title=f"📊 Statistics for {username}#{tag}",
            description=f"Performance over the last {days} days ({total_matches} matches)",
            color=0x00FF00,
            timestamp=datetime.now(timezone.utc)
        )
        
        # Performance stats
        stats_embed.add_field(
            name="🎯 Performance",
            value=(
                f"**Win Rate:** {win_rate}% ({wins}W/{total_matches-wins}L)\n"
                f"**Average K/D/A:** {avg_kills}/{avg_deaths}/{avg_assists}\n"
                f"**Overall K/D:** {overall_kda}\n"
                f"**Average ACS:** {avg_acs}\n"
                f"**Average ADR:** {avg_adr}\n"
                f"**Average KAST:** {avg_kast}%"
            ),
            inline=True
        )
        
        # Combat stats
        stats_embed.add_field(
            name="⚔️ Combat",
            value=(
                f"**Total Kills:** {total_kills:,}\n"
                f"**Total Deaths:** {total_deaths:,}\n"
                f"**Total Damage:** {total_damage:,}\n"
                f"**Avg HS%:** {avg_hs_pct}%\n"
                f"**First Bloods:** {total_first_bloods}"
            ),
            inline=True
        )
        
        # Additional stats
        stats_embed.add_field(
            name="🏆 Achievements",
            value=(
                f"**Multi-kills:** {total_multikills}\n"
                f"**Best Map:** {most_played_map[0]} ({most_played_map[1]}x)\n"
                f"**Main Agent:** {most_played_agent[0]} ({most_played_agent[1]}x)\n"
                f"**Matches Played:** {total_matches}\n"
                f"**Days Active:** {len(set(m['match']['timestamp'][:10] for m in player_matches))}"
            ),
            inline=True
        )
        
        # Recent trend (last 5 matches)
        if total_matches >= 5:
            recent_matches = player_matches[:5]
            recent_acs = [m['player_stats']['stats']['acs'] for m in recent_matches]
            recent_kda = [m['player_stats']['stats']['kda'] for m in recent_matches]
            
            trend_text = "📈 **Recent Trend (Last 5 matches):**\n"
            for i, match_data in enumerate(recent_matches):
                match_info = match_data['match']['match_info']
                player_stats = match_data['player_stats']['stats']
                result = "W" if ((match_data['player_stats']['team'].lower() == 'red' and match_info['red_rounds'] > match_info['blue_rounds']) or 
                               (match_data['player_stats']['team'].lower() == 'blue' and match_info['blue_rounds'] > match_info['red_rounds'])) else "L"
                trend_text += f"`{result}` {player_stats['acs']} ACS, {player_stats['kills']}/{player_stats['deaths']}/{player_stats['assists']} on {match_info['map']}\n"
            
            stats_embed.add_field(name="📈 Recent Performance", value=trend_text, inline=False)
        
        stats_embed.set_footer(text="Statistics tracked from /fullmatch command usage")
        
        await interaction.edit_original_response(content="", embed=stats_embed)
        
    except Exception as e:
        logging.error(f"Error in stats command: {e}")
        await interaction.edit_original_response(content="❌ Error processing statistics data.")

@bot.tree.command(name="link", description="Link your Discord account to your Valorant username")
@app_commands.describe(
    username="Your Valorant username (case-sensitive, spaces allowed)",
    tag="Your Valorant tag (letters and numbers, e.g., 1234 or ABC1)"
)
async def slash_link_account(interaction: discord.Interaction, username: str, tag: str):
    """Link Discord account to Valorant username"""
    
    # Validate tag
    if not tag.isalnum() or len(tag) > 10:
        error_embed = discord.Embed(
            title="❌ Invalid Tag Format",
            description=f"Tag should contain only letters and numbers (max 10 characters)",
            color=0xFF0000
        )
        await interaction.response.send_message(embed=error_embed, ephemeral=True)
        return
    
    try:
        # Create/load user links file
        links_file = "user_links.json"
        user_links = {}
        
        if os.path.exists(links_file):
            try:
                with open(links_file, 'r', encoding='utf-8') as f:
                    user_links = json.load(f)
            except json.JSONDecodeError:
                user_links = {}
        
        # Link the user
        discord_id = str(interaction.user.id)
        valorant_account = f"{username}#{tag}"
        
        user_links[discord_id] = {
            'valorant_username': username,
            'valorant_tag': tag,
            'valorant_full': valorant_account,
            'discord_username': interaction.user.display_name,
            'linked_at': datetime.now(timezone.utc).isoformat(),
            'guild_id': str(interaction.guild.id) if interaction.guild else None
        }
        
        # Save the links
        with open(links_file, 'w', encoding='utf-8') as f:
            json.dump(user_links, f, indent=2, ensure_ascii=False)
        
        success_embed = discord.Embed(
            title="✅ Account Linked Successfully",
            description=f"Your Discord account has been linked to **{valorant_account}**",
            color=0x00FF00
        )
        success_embed.add_field(
            name="📊 Benefits",
            value=(
                "• Your matches will be tracked in server statistics\n"
                "• You'll appear in server leaderboards\n"
                "• Server stats will only show verified members\n"
                "• Use `/unlink` to remove this connection"
            ),
            inline=False
        )
        
        await interaction.response.send_message(embed=success_embed, ephemeral=True)
        
    except Exception as e:
        logging.error(f"Error linking account: {e}")
        await interaction.response.send_message("❌ Error linking account. Please try again later.", ephemeral=True)

@bot.tree.command(name="unlink", description="Unlink your Discord account from your Valorant username")
async def slash_unlink_account(interaction: discord.Interaction):
    """Unlink Discord account from Valorant username"""
    
    try:
        links_file = "user_links.json"
        
        if not os.path.exists(links_file):
            await interaction.response.send_message("❌ You don't have any linked accounts.", ephemeral=True)
            return
        
        with open(links_file, 'r', encoding='utf-8') as f:
            user_links = json.load(f)
        
        discord_id = str(interaction.user.id)
        
        if discord_id not in user_links:
            await interaction.response.send_message("❌ You don't have any linked accounts.", ephemeral=True)
            return
        
        # Get the account info before removing
        valorant_account = user_links[discord_id]['valorant_full']
        
        # Remove the link
        del user_links[discord_id]
        
        # Save the updated links
        with open(links_file, 'w', encoding='utf-8') as f:
            json.dump(user_links, f, indent=2, ensure_ascii=False)
        
        success_embed = discord.Embed(
            title="✅ Account Unlinked",
            description=f"Your Discord account has been unlinked from **{valorant_account}**",
            color=0x00FF00
        )
        success_embed.add_field(
            name="ℹ️ Note",
            value="You can re-link your account anytime using `/link`",
            inline=False
        )
        
        await interaction.response.send_message(embed=success_embed, ephemeral=True)
        
    except Exception as e:
        logging.error(f"Error unlinking account: {e}")
        await interaction.response.send_message("❌ Error unlinking account. Please try again later.", ephemeral=True)

@bot.tree.command(name="serverstats", description="View server-wide statistics from all logged matches")
@app_commands.describe(
    days="Number of days to analyze (default: 30, max: 90)",
    server_only="Show only linked server members (default: True)"
)
async def slash_server_stats(interaction: discord.Interaction, days: int = 30, server_only: bool = True):
    """View server-wide statistics from logged match data"""
    
    if days < 1 or days > 90:
        days = 30
    
    await interaction.response.send_message("🔍 Analyzing server-wide match data...", ephemeral=False)
    
    try:
        # Load user links if server_only is True
        linked_users = set()
        if server_only:
            links_file = "user_links.json"
            if os.path.exists(links_file):
                try:
                    with open(links_file, 'r', encoding='utf-8') as f:
                        user_links = json.load(f)
                        # Get current server members only
                        for discord_id, link_data in user_links.items():
                            if (interaction.guild and 
                                link_data.get('guild_id') == str(interaction.guild.id)):
                                linked_users.add(link_data['valorant_full'])
                except json.JSONDecodeError:
                    pass
            
            if not linked_users:
                no_links_embed = discord.Embed(
                    title="📊 No Linked Server Members",
                    description="No server members have linked their Valorant accounts yet.",
                    color=0xFFFF00
                )
                no_links_embed.add_field(
                    name="💡 Getting Started",
                    value=(
                        "Server members can link their accounts using:\n"
                        "`/link username:YourName tag:1234`\n\n"
                        "Or run `/serverstats server_only:False` to see all tracked players."
                    ),
                    inline=False
                )
                await interaction.edit_original_response(embed=no_links_embed)
                return
    
        
        # Collect data from log files
        logs_dir = "match_logs"
        all_matches = []
        all_players = []
        
        if not os.path.exists(logs_dir):
            await interaction.edit_original_response(content="❌ No match data found. Play some matches with `/fullmatch` first!")
            return
        
        # Check last N days
        for i in range(days):
            date = (datetime.now(timezone.utc) - timedelta(days=i)).strftime('%Y-%m-%d')
            log_file = os.path.join(logs_dir, f"matches_{date}.json")
            
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        daily_matches = json.load(f)
                        all_matches.extend(daily_matches)
                        for match in daily_matches:
                            for player in match.get('players', []):
                                # Filter by linked users if server_only is True  
                                player_name = player.get('name', 'Unknown')
                                player_tag = player.get('tag', '0000')
                                player_full = f"{player_name}#{player_tag}"
                                if not server_only or player_full in linked_users:
                                    all_players.append(player)
                except json.JSONDecodeError:
                    continue
        
        if not all_matches:
            await interaction.edit_original_response(content=f"❌ No match data found in the last {days} days.")
            return
        
        if server_only and not all_players:
            no_data_embed = discord.Embed(
                title="📊 No Server Member Data",
                description=f"No match data found for linked server members in the last {days} days.",
                color=0xFFFF00
            )
            no_data_embed.add_field(
                name="💡 Tips",
                value=(
                    "• Make sure linked members have played matches\n"
                    "• Use `/fullmatch` to track new matches\n"
                    "• Try increasing the day range\n"
                    "• Use `server_only:False` to see all data"
                ),
                inline=False
            )
            await interaction.edit_original_response(embed=no_data_embed)
            return
        
        # Calculate server-wide statistics
        total_matches = len(all_matches)
        unique_players = len(set(f"{p.get('name', 'Unknown')}#{p.get('tag', '0000')}" for p in all_players))
        total_kills = sum(p.get('stats', {}).get('kills', 0) for p in all_players)
        total_deaths = sum(p.get('stats', {}).get('deaths', 0) for p in all_players)
        total_assists = sum(p.get('stats', {}).get('assists', 0) for p in all_players)
        total_damage = sum(p.get('stats', {}).get('damage', 0) for p in all_players)
        total_first_bloods = sum(p.get('stats', {}).get('first_bloods', 0) for p in all_players)
        total_multikills = sum(p.get('stats', {}).get('multikills', 0) for p in all_players)
        
        # Map statistics with win rates
        map_counts = {}
        map_wins = {}
        map_total_rounds = {}
        
        for match in all_matches:
            map_name = match['match_info']['map']
            map_counts[map_name] = map_counts.get(map_name, 0) + 1
            
            # Track wins and total rounds per map
            if map_name not in map_wins:
                map_wins[map_name] = 0
                map_total_rounds[map_name] = 0
            
            # Count wins for linked players if server_only, otherwise count all
            red_rounds = match['match_info']['red_rounds']
            blue_rounds = match['match_info']['blue_rounds']
            
            # For each match, check if any tracked players won
            tracked_players_in_match = []
            for player in match.get('players', []):
                player_name = player.get('name', 'Unknown')
                player_tag = player.get('tag', '0000')
                player_full = f"{player_name}#{player_tag}"
                if not server_only or player_full in linked_users:
                    tracked_players_in_match.append(player)
            
            # Count wins based on tracked players
            for player in tracked_players_in_match:
                team = player.get('team', '').lower()
                if ((team == 'red' and red_rounds > blue_rounds) or 
                    (team == 'blue' and blue_rounds > red_rounds)):
                    map_wins[map_name] += 1
                map_total_rounds[map_name] += 1
        
        # Calculate map win rates
        map_win_rates = {}
        for map_name in map_counts:
            if map_total_rounds.get(map_name, 0) > 0:
                win_rate = round((map_wins.get(map_name, 0) / map_total_rounds[map_name]) * 100, 1)
                map_win_rates[map_name] = win_rate
        
        most_played_maps = sorted(map_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        best_winrate_maps = sorted(
            [(map_name, rate) for map_name, rate in map_win_rates.items() if map_counts[map_name] >= 3],
            key=lambda x: x[1], reverse=True
        )[:5]
        
        # Agent statistics
        agent_counts = {}
        agent_performance = {}
        
        # Process matches to get proper ACS calculations
        for match in all_matches:
            rounds_played = match.get('match_info', {}).get('rounds_played', 0)
            if rounds_played == 0:
                continue  # Skip matches with no rounds data
                
            for player in match.get('players', []):
                # Filter by linked users if server_only is True  
                player_name = player.get('name', 'Unknown')
                player_tag = player.get('tag', '0000')
                player_full = f"{player_name}#{player_tag}"
                if server_only and player_full not in linked_users:
                    continue
                    
                try:
                    # Handle both 'agent' and 'character' fields for backward compatibility
                    agent = player.get('agent') or player.get('character', 'Unknown')
                    if not agent or agent == 'Unknown':
                        # Additional fallback for edge cases
                        agent = 'Unknown'
                    
                    agent_counts[agent] = agent_counts.get(agent, 0) + 1
                    
                    if agent not in agent_performance:
                        agent_performance[agent] = {'kills': 0, 'deaths': 0, 'acs': []}
                    
                    player_stats = player.get('stats', {})
                    agent_performance[agent]['kills'] += player_stats.get('kills', 0)
                    agent_performance[agent]['deaths'] += player_stats.get('deaths', 1)
                    
                    # Recalculate ACS from raw score to fix any bad data
                    raw_score = player_stats.get('score', 0)
                    calculated_acs = int(raw_score / rounds_played) if rounds_played > 0 and raw_score > 0 else 0
                    
                    # Only append reasonable ACS values (0-1000 range)
                    if 0 <= calculated_acs <= 1000:
                        agent_performance[agent]['acs'].append(calculated_acs)
                    
                except Exception as agent_error:
                    logging.error(f"Error processing agent stats for player {player.get('name', 'Unknown')}: {agent_error}")
                    # Continue processing other players
                    continue
        
        # Calculate average performance per agent
        for agent in agent_performance:
            if agent_performance[agent]['acs']:
                agent_performance[agent]['avg_acs'] = round(
                    sum(agent_performance[agent]['acs']) / len(agent_performance[agent]['acs']), 1
                )
                agent_performance[agent]['avg_kd'] = round(
                    agent_performance[agent]['kills'] / agent_performance[agent]['deaths'], 2
                ) if agent_performance[agent]['deaths'] > 0 else 0
        
        most_played_agents = sorted(agent_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        best_performing_agents = sorted(
            [(agent, data['avg_acs']) for agent, data in agent_performance.items() if data.get('avg_acs', 0) > 0],
            key=lambda x: x[1], reverse=True
        )[:5]
        
        # Top performers with enhanced tracking - recalculate ACS from raw data
        player_stats = {}
        player_best_agents = {}  # Track best agent per player
        player_best_games = {}   # Track best game per player
        
        # Process by match to get rounds_played context for accurate ACS calculation
        for match in all_matches:
            match_info = match.get('match_info', {})
            rounds_played = match_info.get('rounds_played', 0)
            if rounds_played == 0:
                continue  # Skip matches without round data
            
            match_players = match.get('players', [])
            for player in match_players:
                try:
                    player_name = player.get('name', 'Unknown') 
                    player_tag = player.get('tag', '0000')
                    player_id = f"{player_name}#{player_tag}"
                    
                    # Handle both 'agent' and 'character' fields for backward compatibility
                    agent = player.get('agent') or player.get('character', 'Unknown')
                    if not agent or agent == 'Unknown':
                        agent = 'Unknown'
                    
                    # Initialize player stats
                    if player_id not in player_stats:
                        player_stats[player_id] = {
                            'matches': 0, 'kills': 0, 'deaths': 0, 'assists': 0,
                            'acs_total': 0, 'damage': 0, 'first_bloods': 0, 'multikills': 0, 'kast_total': 0,
                            'agents': {}  # Track performance per agent
                        }
                    
                    # Initialize player best tracking
                    if player_id not in player_best_agents:
                        player_best_agents[player_id] = {}
                    if player_id not in player_best_games:
                        player_best_games[player_id] = {
                            'acs': 0, 'kills': 0, 'agent': 'Unknown', 'map': 'Unknown', 'kda': 0
                        }
                    
                    # Get player stats with safe access
                    stats = player_stats[player_id]
                    player_stats_data = player.get('stats', {})
                    
                    # Basic stats
                    kills = player_stats_data.get('kills', 0)
                    deaths = player_stats_data.get('deaths', 0)
                    assists = player_stats_data.get('assists', 0)
                    total_score = player_stats_data.get('score', 0)
                    damage = player_stats_data.get('damage', 0)
                    
                    # Recalculate ACS from raw data
                    current_acs = int(total_score / rounds_played) if rounds_played > 0 and total_score > 0 else 0
                    # Validate ACS range (0-1000 is reasonable)
                    if current_acs > 1000:
                        current_acs = 0  # Invalid data, skip
                    
                    # Update player stats
                    stats['matches'] += 1
                    stats['kills'] += kills
                    stats['deaths'] += deaths
                    stats['assists'] += assists
                    stats['acs_total'] += current_acs  # Use recalculated ACS
                    stats['damage'] += damage
                    stats['first_bloods'] += player_stats_data.get('first_bloods', 0)
                    stats['multikills'] += player_stats_data.get('multikills', 0)
                    stats['kast_total'] += player_stats_data.get('kast', 0)
                    
                    # Track agent performance for this player
                    if agent not in stats['agents']:
                        stats['agents'][agent] = {'matches': 0, 'acs_total': 0, 'kills': 0, 'deaths': 0}
                    stats['agents'][agent]['matches'] += 1
                    stats['agents'][agent]['acs_total'] += current_acs  # Use recalculated ACS
                    stats['agents'][agent]['kills'] += kills
                    stats['agents'][agent]['deaths'] += deaths
                    
                    # Track best game for this player
                    if current_acs > player_best_games[player_id]['acs']:
                        current_kda = round((kills + assists) / deaths, 2) if deaths > 0 else float('inf')
                        player_best_games[player_id] = {
                            'acs': current_acs,
                            'kills': kills,
                            'deaths': deaths,
                            'assists': assists,
                            'agent': agent,
                            'map': match_info.get('map', 'Unknown'),
                            'kda': current_kda
                        }
                        
                except Exception as player_error:
                    logging.error(f"Error processing player stats for {player.get('name', 'Unknown')}#{player.get('tag', '0000')}: {player_error}")
                    # Continue processing other players
                    continue
        
        # Calculate best agent per player
        for player_id, stats in player_stats.items():
            best_agent = None
            best_avg_acs = 0
            
            for agent, agent_stats in stats['agents'].items():
                if agent_stats['matches'] >= 2:  # Minimum 2 matches with agent
                    avg_acs = agent_stats['acs_total'] / agent_stats['matches']
                    if avg_acs > best_avg_acs:
                        best_avg_acs = avg_acs
                        best_agent = agent
            
            if best_agent:
                player_best_agents[player_id] = {
                    'agent': best_agent,
                    'avg_acs': round(best_avg_acs, 1),
                    'matches': stats['agents'][best_agent]['matches']
                }
        
        # Calculate averages and sort
        for player_id in player_stats:
            stats = player_stats[player_id]
            if stats['matches'] > 0:
                stats['avg_acs'] = round(stats['acs_total'] / stats['matches'], 1)
                stats['avg_kd'] = round(stats['kills'] / stats['deaths'], 2) if stats['deaths'] > 0 else 0
                stats['avg_kast'] = round(stats['kast_total'] / stats['matches'], 1)
        
        # Top players (minimum 3 matches)
        eligible_players = {pid: stats for pid, stats in player_stats.items() if stats['matches'] >= 3}
        top_acs_players = sorted(eligible_players.items(), key=lambda x: x[1]['avg_acs'], reverse=True)[:5]
        top_kd_players = sorted(eligible_players.items(), key=lambda x: x[1]['avg_kd'], reverse=True)[:5]
        top_kast_players = sorted(eligible_players.items(), key=lambda x: x[1]['avg_kast'], reverse=True)[:5]
        top_damage_players = sorted(eligible_players.items(), key=lambda x: x[1]['damage'], reverse=True)[:5]
        
        # Create server statistics embed
        server_type = "Server Members" if server_only else "All Players"
        stats_embed = discord.Embed(
            title=f"🌐 {server_type} Statistics",
            description=f"Analytics from the last {days} days" + (f" ({len(linked_users)} linked members)" if server_only else ""),
            color=0x7289DA,
            timestamp=datetime.now(timezone.utc)
        )
        
        # Overview stats
        stats_embed.add_field(
            name="📊 Overview",
            value=(
                f"**Matches Tracked:** {total_matches:,}\n"
                f"**Unique Players:** {unique_players:,}\n"
                f"**Total Kills:** {total_kills:,}\n"
                f"**Total Deaths:** {total_deaths:,}\n"
                f"**Total Damage:** {total_damage:,}\n"
                f"**First Bloods:** {total_first_bloods:,}\n"
                f"**Multi-kills:** {total_multikills:,}"
            ),
            inline=True
        )
        
        # Map statistics with win rates
        map_text = "\n".join([f"{i+1}. **{map_name}** ({count} matches)" 
                             for i, (map_name, count) in enumerate(most_played_maps)])
        stats_embed.add_field(
            name="🗺️ Most Played Maps",
            value=map_text or "No data",
            inline=True
        )
        
        # Map win rates
        if best_winrate_maps:
            winrate_text = "\n".join([f"{i+1}. **{map_name}** ({win_rate}% wins)" 
                                     for i, (map_name, win_rate) in enumerate(best_winrate_maps)])
            stats_embed.add_field(
                name="🏆 Best Map Win Rates",
                value=winrate_text,
                inline=True
            )
        else:
            stats_embed.add_field(
                name="🏆 Best Map Win Rates",
                value="No data (min 3 matches)",
                inline=True
            )
        
        # Agent pick rates
        agent_text = "\n".join([f"{i+1}. **{agent}** ({count} picks)" 
                               for i, (agent, count) in enumerate(most_played_agents)])
        stats_embed.add_field(
            name="🎭 Agent Pick Rates",
            value=agent_text or "No data",
            inline=True
        )
        
        # Best performing agents
        if best_performing_agents:
            agent_perf_text = "\n".join([f"{i+1}. **{agent}** ({avg_acs} avg ACS)" 
                                        for i, (agent, avg_acs) in enumerate(best_performing_agents)])
            stats_embed.add_field(
                name="⭐ Best Performing Agents",
                value=agent_perf_text,
                inline=True
            )
        
        # Top ACS players
        if top_acs_players:
            acs_text = "\n".join([f"{i+1}. **{player}** ({stats['avg_acs']} ACS)" 
                                 for i, (player, stats) in enumerate(top_acs_players)])
            stats_embed.add_field(
                name="🏆 Top ACS Players",
                value=acs_text,
                inline=True
            )
        
        # Top K/D players
        if top_kd_players:
            kd_text = "\n".join([f"{i+1}. **{player}** ({stats['avg_kd']} K/D)" 
                                for i, (player, stats) in enumerate(top_kd_players)])
            stats_embed.add_field(
                name="⚔️ Top K/D Players",
                value=kd_text,
                inline=True
            )
        
        # Top KAST players
        if top_kast_players:
            kast_text = "\n".join([f"{i+1}. **{player}** ({stats['avg_kast']}% KAST)" 
                                  for i, (player, stats) in enumerate(top_kast_players)])
            stats_embed.add_field(
                name="🎯 Top KAST Players",
                value=kast_text,
                inline=True
            )
        
        # Best agents per player (top 5 by avg ACS with their best agent)
        best_agent_players = []
        for player_id, stats in player_stats.items():
            if stats['matches'] >= 3 and player_id in player_best_agents:
                try:
                    best_agent_info = player_best_agents[player_id]
                    # Safe access to agent field with fallback
                    agent = best_agent_info.get('agent') or best_agent_info.get('character', 'Unknown')
                    avg_acs = best_agent_info.get('avg_acs', 0)
                    matches = best_agent_info.get('matches', 0)
                    
                    if agent and avg_acs > 0:
                        best_agent_players.append((
                            player_id, 
                            agent, 
                            avg_acs,
                            matches
                        ))
                except Exception as e:
                    logging.error(f"Error processing best agent for {player_id}: {e}")
                    continue
        
        best_agent_players.sort(key=lambda x: x[2], reverse=True)  # Sort by avg ACS
        top_agent_players = best_agent_players[:5]
        
        if top_agent_players:
            agent_text = "\n".join([f"{i+1}. **{player}**\n   {agent} ({avg_acs} ACS, {matches}m)" 
                                   for i, (player, agent, avg_acs, matches) in enumerate(top_agent_players)])
            stats_embed.add_field(
                name="⭐ Best Agent Per Player",
                value=agent_text,
                inline=True
            )
        
        # Best games (top 5 highest ACS games)
        best_games = []
        for player_id, game_info in player_best_games.items():
            try:
                acs = game_info.get('acs', 0)
                if acs > 0:  # Has a recorded game
                    best_games.append((
                        player_id,
                        acs,
                        game_info.get('kills', 0),
                        game_info.get('deaths', 0),
                        game_info.get('assists', 0),
                        game_info.get('agent') or game_info.get('character', 'Unknown'),
                        game_info.get('kda', 0)
                    ))
            except Exception as e:
                logging.error(f"Error processing best game for {player_id}: {e}")
                continue
        
        best_games.sort(key=lambda x: x[1], reverse=True)  # Sort by ACS
        top_games = best_games[:5]
        
        if top_games:
            games_text = "\n".join([f"{i+1}. **{player}**\n   {acs} ACS ({k}/{d}/{a}) {agent}" 
                                   for i, (player, acs, k, d, a, agent, kda) in enumerate(top_games)])
            stats_embed.add_field(
                name="🔥 Best Individual Games",
                value=games_text,
                inline=True
            )
        
        # NEW ANALYTICS: First Blood Win Rate Analysis
        try:
            first_blood_stats = analyze_first_blood_win_rate(all_matches)
            if first_blood_stats['first_blood_rounds'] > 0:
                fb_text = (
                    f"**Win Rate with First Blood:** {first_blood_stats['win_rate']}%\n"
                    f"**Rounds with First Blood:** {first_blood_stats['first_blood_rounds']:,}\n"
                    f"**First Blood Wins:** {first_blood_stats['first_blood_wins']:,}\n"
                    f"**First Blood Losses:** {first_blood_stats['first_blood_losses']:,}\n"
                    f"**Sample Size:** {first_blood_stats['total_rounds']:,} rounds"
                )
                stats_embed.add_field(
                    name="🎯 First Blood Analysis",
                    value=fb_text,
                    inline=True
                )
        except Exception as fb_error:
            logging.error(f"Error in first blood analysis: {fb_error}")
        
        # NEW ANALYTICS: Most Effective Loadouts
        try:
            effective_loadouts = analyze_effective_loadouts(all_matches)
            if effective_loadouts:
                loadout_lines = []
                for i, (signature, stats) in enumerate(list(effective_loadouts.items())[:3]):
                    efficiency = round((stats['win_rate'] / max(stats['total_value'], 1)) * 100, 2)
                    weapons_display = ", ".join(stats['primary_weapons'][:2])
                    loadout_lines.append(
                        f"{i+1}. **{weapons_display}** (A{stats['armor_count']})\n"
                        f"   ${stats['total_value']:,} • {stats['win_rate']}% WR • {stats['total_rounds']} rounds"
                    )
                
                loadout_text = "\n".join(loadout_lines) if loadout_lines else "Insufficient data"
                stats_embed.add_field(
                    name="💰 Most Effective Loadouts",
                    value=loadout_text,
                    inline=True
                )
            else:
                stats_embed.add_field(
                    name="💰 Most Effective Loadouts", 
                    value="Insufficient economy data",
                    inline=True
                )
        except Exception as loadout_error:
            logging.error(f"Error in loadout analysis: {loadout_error}")
            stats_embed.add_field(
                name="💰 Most Effective Loadouts", 
                value="Economy analysis unavailable",
                inline=True
            )
        
        # Server averages
        if len(all_players) > 0:
            # Calculate proper averages per unique player using our corrected player_stats
            if player_stats:
                # Calculate average ACS per unique player (already using recalculated values)
                player_avg_acs_list = []
                player_avg_kast_list = []
                for player_id, stats in player_stats.items():
                    if stats['matches'] > 0:
                        player_avg_acs = stats['acs_total'] / stats['matches']
                        player_avg_kast = stats['kast_total'] / stats['matches']
                        player_avg_acs_list.append(player_avg_acs)
                        player_avg_kast_list.append(player_avg_kast)
                
                avg_acs = round(sum(player_avg_acs_list) / len(player_avg_acs_list), 1) if player_avg_acs_list else 0
                avg_kast = round(sum(player_avg_kast_list) / len(player_avg_kast_list), 1) if player_avg_kast_list else 0
            else:
                # Fallback: recalculate from raw data instead of using corrupted ACS
                valid_players = []
                for player in all_players:
                    player_stats_data = player.get('stats', {})
                    total_score = player_stats_data.get('score', 0)
                    # Find the match context for this player to get rounds_played
                    player_match = None
                    for match in all_matches:
                        if any(p.get('name') == player.get('name') and p.get('tag') == player.get('tag') 
                              for p in match.get('players', [])):
                            player_match = match
                            break
                    
                    if player_match:
                        rounds_played = player_match.get('match_info', {}).get('rounds_played', 0)
                        if rounds_played > 0 and total_score > 0:
                            calculated_acs = int(total_score / rounds_played)
                            if 0 <= calculated_acs <= 1000:  # Validate range
                                valid_players.append({
                                    'acs': calculated_acs,
                                    'kast': player_stats_data.get('kast', 0)
                                })
                
                if valid_players:
                    avg_acs = round(sum(p['acs'] for p in valid_players) / len(valid_players), 1)
                    avg_kast = round(sum(p['kast'] for p in valid_players) / len(valid_players), 1)
                else:
                    avg_acs = 0
                    avg_kast = 0
            
            avg_kd = round(total_kills / total_deaths, 2) if total_deaths > 0 else 0
            avg_damage_per_player = round(total_damage / len(all_players), 0)
            
            stats_embed.add_field(
                name="📈 Server Averages",
                value=(
                    f"**Average ACS:** {avg_acs}\n"
                    f"**Average K/D:** {avg_kd}\n"
                    f"**Average KAST:** {avg_kast}%\n"
                    f"**Avg Damage/Player:** {avg_damage_per_player:,}\n"
                    f"**Matches per Player:** {round(total_matches / unique_players, 1)}"
                ),
                inline=False
            )
        
        filter_note = " • Server members only" if server_only else " • All tracked players"
        stats_embed.set_footer(text=f"Data from {total_matches} matches • Last {days} days{filter_note}")
        
        await interaction.edit_original_response(content="", embed=stats_embed)
        
    except Exception as e:
        import traceback
        error_traceback = traceback.format_exc()
        logging.error(f"Error in serverstats command: {e}")
        logging.error(f"Full traceback: {error_traceback}")
        await interaction.edit_original_response(content="❌ Error processing server statistics data.")

@bot.tree.command(name="economy", description="Analyze team economy and loadout effectiveness")
@app_commands.describe(
    days="Number of days to analyze (default: 30, max: 90)",
    server_only="Show only linked server members (default: True)"
)
async def slash_economy_analysis(interaction: discord.Interaction, days: int = 30, server_only: bool = True):
    """Detailed economy and loadout analysis"""
    
    if days < 1 or days > 90:
        days = 30
    
    await interaction.response.defer()
    
    try:
        # Load user links if server_only is True
        linked_users = set()
        if server_only:
            links_file = "user_links.json"
            if os.path.exists(links_file):
                try:
                    with open(links_file, 'r', encoding='utf-8') as f:
                        user_links = json.load(f)
                        for discord_id, link_data in user_links.items():
                            if link_data.get('guild_id') == str(interaction.guild.id) if interaction.guild else True:
                                linked_users.add(link_data['valorant_full'])
                except json.JSONDecodeError:
                    pass
        
        # Collect match data
        logs_dir = "match_logs"
        all_matches = []
        
        if not os.path.exists(logs_dir):
            await interaction.edit_original_response(content="❌ No match data found.")
            return
        
        for i in range(days):
            date = (datetime.now(timezone.utc) - timedelta(days=i)).strftime('%Y-%m-%d')
            log_file = os.path.join(logs_dir, f"matches_{date}.json")
            
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        daily_data = json.load(f)
                        for entry in daily_data:
                            if server_only:
                                # Filter matches with linked players
                                has_linked_player = False
                                if 'match_data' in entry and 'data' in entry['match_data']:
                                    for player in entry['match_data']['data']['players']['all_players']:
                                        player_name = f"{player.get('name', 'Unknown')}#{player.get('tag', '0000')}"
                                        if player_name in linked_users:
                                            has_linked_player = True
                                            break
                                if has_linked_player:
                                    all_matches.append(entry)
                            else:
                                all_matches.append(entry)
                except json.JSONDecodeError:
                    continue
        
        if not all_matches:
            await interaction.edit_original_response(content="❌ No match data found for analysis.")
            return
        
        # Create processing status embed
        processing_embed = discord.Embed(
            title="💰 Processing Economy Data",
            description=f"Analyzing {len(all_matches)} matches from the last {days} days...",
            color=0x3498DB
        )
        await interaction.edit_original_response(embed=processing_embed)
        
        # Process data in smaller chunks to avoid timeout
        await asyncio.sleep(0.1)  # Small delay to prevent overwhelming
        
        # Update progress
        processing_embed.description = f"Analyzing first blood data from {len(all_matches)} matches..."
        await interaction.edit_original_response(embed=processing_embed)
        
        # Analyze data in chunks to prevent timeouts
        first_blood_stats = analyze_first_blood_win_rate(all_matches)
        
        # Update progress
        processing_embed.description = f"Analyzing loadout effectiveness from {len(all_matches)} matches..."
        await interaction.edit_original_response(embed=processing_embed)
        await asyncio.sleep(0.1)
        
        effective_loadouts = analyze_effective_loadouts(all_matches)
        
        # Final progress update
        processing_embed.description = f"Generating economy analysis report..."
        await interaction.edit_original_response(embed=processing_embed)
        await asyncio.sleep(0.1)
        
        # Create detailed economy embed
        economy_embed = discord.Embed(
            title="💰 Economy & Tactical Analysis",
            description=f"Detailed analysis from the last {days} days ({len(all_matches)} matches)",
            color=0xFFD700,
            timestamp=datetime.now(timezone.utc)
        )
        
        # First Blood Analysis
        if first_blood_stats['first_blood_rounds'] > 0:
            fb_impact = "High Impact" if first_blood_stats['win_rate'] > 65 else "Moderate Impact" if first_blood_stats['win_rate'] > 55 else "Low Impact"
            economy_embed.add_field(
                name="🎯 First Blood Impact",
                value=(
                    f"**Win Rate:** {first_blood_stats['win_rate']}% ({fb_impact})\n"
                    f"**Total Rounds:** {first_blood_stats['total_rounds']:,}\n"
                    f"**First Blood Rounds:** {first_blood_stats['first_blood_rounds']:,}\n"
                    f"**Wins/Losses:** {first_blood_stats['first_blood_wins']:,}W/{first_blood_stats['first_blood_losses']:,}L\n"
                    f"**Advantage:** +{first_blood_stats['win_rate'] - 50:.1f}% over base 50%"
                ),
                inline=False
            )
        
        # Effective Loadouts Analysis
        if effective_loadouts:
            loadout_details = []
            for i, (signature, stats) in enumerate(list(effective_loadouts.items())[:5]):
                efficiency_score = round((stats['win_rate'] / max(stats['total_value'], 1)) * 1000, 2)
                cost_per_win = round(stats['total_value'] / max(stats['wins'], 1), 0)
                
                weapons = stats['primary_weapons'][:2]
                weapons_str = " + ".join(weapons) if len(weapons) > 1 else weapons[0] if weapons else "Mixed"
                
                loadout_details.append(
                    f"**{i+1}. {weapons_str}** (Armor: {stats['armor_count']}/5)\n"
                    f"   💰 Cost: ${stats['total_value']:,} | 🏆 Win Rate: {stats['win_rate']}%\n"
                    f"   📊 Efficiency: {efficiency_score} | 💸 Cost/Win: ${cost_per_win:,}\n"
                    f"   🎲 Sample: {stats['total_rounds']} rounds ({stats['wins']}W/{stats['total_rounds']-stats['wins']}L)"
                )
            
            economy_embed.add_field(
                name="🏆 Most Effective Loadouts",
                value="\n\n".join(loadout_details),
                inline=False
            )
            
            # Economy efficiency insights
            best_loadout = list(effective_loadouts.values())[0]
            worst_loadout = list(effective_loadouts.values())[-1]
            
            economy_embed.add_field(
                name="📈 Economic Insights",
                value=(
                    f"**Best Efficiency:** {best_loadout['win_rate']}% win rate at ${best_loadout['total_value']:,}\n"
                    f"**Highest Cost/Win:** ${round(worst_loadout['total_value'] / max(worst_loadout['wins'], 1)):,}\n"
                    f"**Avg Loadout Value:** ${round(sum(s['total_value'] for s in effective_loadouts.values()) / len(effective_loadouts)):,}\n"
                    f"**Economy Analyzed:** {sum(s['total_rounds'] for s in effective_loadouts.values())} qualifying rounds"
                ),
                inline=True
            )
        else:
            economy_embed.add_field(
                name="💰 Loadout Analysis",
                value="Insufficient economy data available.\nThis feature requires detailed round economy data from the API.",
                inline=False
            )
        
        # Analysis notes
        economy_embed.add_field(
            name="📋 Analysis Notes",
            value=(
                "• First blood analysis includes all rounds with recorded kill events\n"
                "• Loadout analysis excludes rounds 1, 2, 13, 14 (pistol rounds)\n"
                "• Efficiency = (Win Rate ÷ Cost) × 1000\n"
                "• Minimum 5 rounds required for loadout inclusion\n"
                "• Economy data availability depends on API coverage"
            ),
            inline=False
        )
        
        filter_note = " • Server members only" if server_only else " • All tracked players"
        economy_embed.set_footer(text=f"Economy data from {len(all_matches)} matches • Last {days} days{filter_note}")
        
        await interaction.edit_original_response(content="", embed=economy_embed)
        
    except discord.NotFound:
        # Interaction token expired
        logging.error("Economy command: Interaction token expired")
        # Can't respond to expired interaction
        return
    except Exception as e:
        logging.error(f"Error in economy analysis: {e}")
        try:
            await interaction.edit_original_response(content="❌ Error processing economy data.")
        except discord.NotFound:
            logging.error("Economy command: Could not send error response - interaction expired")

@bot.tree.command(name="kastdebug", description="Debug KAST calculation for a specific match")
@app_commands.describe(
    match_id="The match ID to debug KAST calculation for",
    username="Player's username",
    tag="Player's tag"
)
async def slash_kast_debug(interaction: discord.Interaction, match_id: str, username: str, tag: str):
    """Debug KAST calculation for a specific match"""
    
    await interaction.response.send_message("🔍 Analyzing KAST calculation...", ephemeral=False)
    
    try:
        # Fetch detailed match data
        match_data = await asyncio.get_event_loop().run_in_executor(
            None, fetch_match_details, match_id
        )
        
        if not match_data:
            await interaction.edit_original_response(content="❌ Could not fetch match data for the provided match ID.")
            return
        
        # Find the player
        players_data = match_data.get('players', {})
        if isinstance(players_data, dict):
            all_players = players_data.get('all_players', [])
        else:
            all_players = []
        
        target_player = None
        for player in all_players:
            if (player.get('name', '').lower() == username.lower() and 
                player.get('tag', '') == tag):
                target_player = player
                break
        
        if not target_player:
            await interaction.edit_original_response(content=f"❌ Player {username}#{tag} not found in this match.")
            return
        
        player_puuid = target_player.get('puuid', '')
        metadata = match_data.get('metadata', {})
        total_rounds = metadata.get('rounds_played', 0)
        
        # Detailed KAST analysis
        round_data = match_data.get('rounds', [])
        match_kills = match_data.get('kills', [])
        
        # Group kills by round for trade detection
        kills_by_round = {}
        for kill_event in match_kills:
            round_num = kill_event.get('round', 0)
            if round_num not in kills_by_round:
                kills_by_round[round_num] = []
            kills_by_round[round_num].append(kill_event)
        
        debug_text = f"**KAST Debug for {username}#{tag}**\n"
        debug_text += f"Match ID: {match_id}\n"
        debug_text += f"Total rounds: {total_rounds}\n"
        debug_text += f"Round data entries: {len(round_data)}\n"
        debug_text += f"Kill events: {len(match_kills)}\n\n"
        
        kast_rounds = 0
        rounds_checked = 0
        
        for round_info in round_data:
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
            assist_details = ""
            
            # Also check kill events for assists in this round  
            round_kills_list = (kills_by_round.get(round_num, []) or 
                               kills_by_round.get(round_num + 1, []) or 
                               kills_by_round.get(round_num - 1, []))
            
            assist_from_events = False
            for kill_event in round_kills_list:
                assistants = kill_event.get('assistants', [])
                if assistants and any(assist.get('assistant_puuid') == player_puuid for assist in assistants):
                    assist_from_events = True
                    assist_details = " (found in kill events)"
                    break
            
            # Update assist status if found in events
            if not has_assist and assist_from_events:
                has_assist = True
                assist_details = " (only in kill events)"
            elif has_assist and assist_from_events:
                assist_details = " (in both round stats and kill events)"
            elif has_assist and not assist_from_events:
                assist_details = " (only in round stats)"
            
            # Check for Survival
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
            
            # Check for Trade with detailed analysis
            was_traded = False
            trade_details = ""
            
            round_kills_list = (kills_by_round.get(round_num, []) or 
                               kills_by_round.get(round_num + 1, []) or 
                               kills_by_round.get(round_num - 1, []))
            
            if round_kills_list:
                round_kills_list.sort(key=lambda x: x.get('kill_time_in_round', 0))
                
                for i, kill_event in enumerate(round_kills_list):
                    if kill_event.get('victim_puuid') == player_puuid:
                        player_death_time = kill_event.get('kill_time_in_round', 0)
                        killer_puuid = kill_event.get('killer_puuid')
                        
                        # Check if the killer was eliminated by a teammate within 5 seconds
                        for j in range(len(round_kills_list)):
                            if j == i:
                                continue
                                
                            trade_kill = round_kills_list[j]
                            trade_time = trade_kill.get('kill_time_in_round', 0)
                            time_diff = abs(trade_time - player_death_time)
                            
                            if (trade_kill.get('victim_puuid') == killer_puuid and
                                time_diff <= 5000 and
                                trade_kill.get('killer_puuid') != player_puuid):
                                was_traded = True
                                trade_details = f" (Trade: {time_diff}ms after death)"
                                break
                        break
            
            # KAST if any condition is met
            round_kast = has_kill or has_assist or survived or was_traded
            if round_kast:
                kast_rounds += 1
            
            # Format round result
            k_char = "K:True" if has_kill else "K:False"
            a_char = f"A:True{assist_details}" if has_assist else "A:False"
            s_char = "S:True" if survived else "S:False"
            t_char = "T:True" if was_traded else "T:False"
            result = "✅" if round_kast else "❌"
            
            debug_text += f"Round {round_num}: {k_char} {a_char} {s_char} {t_char}{trade_details} = {result}\n"
        
        kast_percentage = round((kast_rounds / rounds_checked) * 100, 1)
        
        debug_text += f"\nResults:\n"
        debug_text += f"Rounds analyzed: {rounds_checked}\n"
        debug_text += f"KAST rounds: {kast_rounds}\n"
        debug_text += f"KAST%: {kast_percentage}%"
        
        # Split into multiple messages if too long
        if len(debug_text) > 2000:
            # Split at reasonable breakpoints
            parts = []
            current_part = ""
            lines = debug_text.split('\n')
            
            for line in lines:
                if len(current_part + line + '\n') > 1900:
                    parts.append(f"```\n{current_part}\n```")
                    current_part = line
                else:
                    current_part += line + '\n'
            
            if current_part:
                parts.append(f"```\n{current_part}\n```")
            
            # Send first part
            await interaction.edit_original_response(content=parts[0])
            
            # Send remaining parts
            for part in parts[1:]:
                await interaction.followup.send(content=part, ephemeral=False)
        else:
            await interaction.edit_original_response(content=f"```\n{debug_text}\n```")
            
    except Exception as e:
        logging.error(f"Error in KAST debug command: {e}")
        await interaction.edit_original_response(content="❌ Error analyzing KAST calculation.")

@bot.tree.command(name="linked", description="Show all server members who have linked their Valorant accounts")
async def slash_show_linked(interaction: discord.Interaction):
    """Show linked server members"""
    
    try:
        links_file = "user_links.json"
        
        if not os.path.exists(links_file):
            await interaction.response.send_message("❌ No linked accounts found.", ephemeral=True)
            return
        
        with open(links_file, 'r', encoding='utf-8') as f:
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
            value="Linked members appear in `/serverstats` and server leaderboards",
            inline=False
        )
        
        await interaction.response.send_message(embed=linked_embed)
        
    except Exception as e:
        logging.error(f"Error showing linked accounts: {e}")
        await interaction.response.send_message("❌ Error retrieving linked accounts.", ephemeral=True)

@bot.tree.command(name="firstblood", description="Analyze first blood win percentage from logged match data")
@app_commands.describe(
    days="Number of days to analyze (1-90, default: 30)",
    server_only="Show only linked server members (default: true)"
)
async def slash_first_blood_analysis(
    interaction: discord.Interaction,
    days: app_commands.Range[int, 1, 90] = 30,
    server_only: bool = True
):
    """Analyze first blood win percentage"""
    
    try:
        await interaction.response.defer()
        
        # Load logged match data
        logs_dir = "match_logs"
        if not os.path.exists(logs_dir):
            await interaction.edit_original_response(content="❌ No match data found. Use `/fullmatch` to start logging matches.")
            return
        
        # Get date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        all_matches = []
        
        # Load matches from date range
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            log_file = os.path.join(logs_dir, f"matches_{date_str}.json")
            
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        day_matches = json.load(f)
                        all_matches.extend(day_matches)
                except json.JSONDecodeError:
                    continue
            
            current_date += timedelta(days=1)
        
        if not all_matches:
            await interaction.edit_original_response(content=f"❌ No match data found for the last {days} days.")
            return
        
        # Filter for server members if requested
        if server_only and interaction.guild:
            links_file = "user_links.json"
            linked_players = set()
            
            if os.path.exists(links_file):
                try:
                    with open(links_file, 'r', encoding='utf-8') as f:
                        user_links = json.load(f)
                    
                    for discord_id, link_data in user_links.items():
                        if link_data.get('guild_id') == str(interaction.guild.id):
                            linked_players.add(link_data['valorant_full'].lower())
                except:
                    pass
            
            if linked_players:
                filtered_matches = []
                for match in all_matches:
                    # Check if any player in the match is linked to this server
                    match_has_linked_player = False
                    if 'match_data' in match and 'data' in match['match_data']:
                        for player in match['match_data']['data']['players']['all_players']:
                            player_name = f"{player.get('name', 'Unknown')}#{player.get('tag', '0000')}".lower()
                            if player_name in linked_players:
                                match_has_linked_player = True
                                break
                    
                    if match_has_linked_player:
                        filtered_matches.append(match)
                
                all_matches = filtered_matches
        
        if not all_matches:
            await interaction.edit_original_response(content=f"❌ No matches found for {'linked server members' if server_only else 'any players'} in the last {days} days.")
            return
        
        # Analyze first blood win rates
        first_blood_stats = analyze_first_blood_win_rate(all_matches)
        
        # Create embed
        embed = discord.Embed(
            title="🩸 First Blood Win Rate Analysis",
            color=0xFF4444
        )
        
        embed.add_field(
            name="📊 Overall Statistics",
            value=(
                f"**Total Rounds:** {first_blood_stats.get('total_rounds', 0):,}\n"
                f"**First Blood Rounds:** {first_blood_stats.get('first_blood_rounds', 0):,}\n"
                f"**First Blood Rate:** {(first_blood_stats.get('first_blood_rounds', 0) / max(first_blood_stats.get('total_rounds', 1), 1) * 100):.1f}%"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🎯 First Blood Impact",
            value=(
                f"**Wins with First Blood:** {first_blood_stats.get('first_blood_wins', 0):,}\n"
                f"**Losses with First Blood:** {first_blood_stats.get('first_blood_losses', 0):,}\n"
                f"**Win Rate with First Blood:** {first_blood_stats.get('win_rate', 0):.1f}%"
            ),
            inline=False
        )
        
        # Add advantage calculation
        if first_blood_stats.get('total_rounds', 0) > 0:
            normal_win_rate = 50.0  # Baseline assumption
            first_blood_win_rate = first_blood_stats.get('win_rate', 0)
            advantage = first_blood_win_rate - normal_win_rate
            
            embed.add_field(
                name="📈 Tactical Advantage",
                value=(
                    f"**Estimated Advantage:** +{advantage:.1f}% win rate\n"
                    f"**Strategic Value:** {'High' if advantage > 15 else 'Medium' if advantage > 5 else 'Low'}\n"
                    f"**Sample Size:** {len(all_matches)} matches analyzed"
                ),
                inline=False
            )
        
        embed.add_field(
            name="⏰ Analysis Period",
            value=(
                f"**Days Analyzed:** {days}\n"
                f"**Data Source:** {'Server members only' if server_only else 'All logged matches'}\n"
                f"**Date Range:** {start_date.strftime('%m/%d/%y')} - {end_date.strftime('%m/%d/%y')}"
            ),
            inline=False
        )
        
        embed.set_footer(text="💡 First blood advantage varies by rank and team coordination")
        
        await interaction.edit_original_response(embed=embed)
        
    except Exception as e:
        logging.error(f"Error in first blood analysis command: {e}")
        await interaction.edit_original_response(content="❌ Error analyzing first blood data.")

@bot.tree.command(name="pullgames", description="Pull and log a specific number of recent games from a player")
@app_commands.describe(
    region="Select player's region",
    username="Player's Valorant username",
    tag="Player's Valorant tag (without #)",
    count="Number of games to pull (1-20, default: 5)"
)
@app_commands.choices(region=[
    app_commands.Choice(name="Europe", value="eu"),
    app_commands.Choice(name="North America", value="na"),
    app_commands.Choice(name="Asia Pacific", value="ap"),
    app_commands.Choice(name="Korea", value="kr")
])
async def slash_pull_games(
    interaction: discord.Interaction,
    region: app_commands.Choice[str],
    username: str,
    tag: str,
    count: app_commands.Range[int, 1, 20] = 5
):
    """Pull and log multiple games from a player for testing/analysis"""
    
    try:
        await interaction.response.defer()
        
        # Create progress embed
        progress_embed = discord.Embed(
            title="📥 Pulling Match Data",
            description=f"Fetching {count} recent games for **{username}#{tag}**...",
            color=0x3498DB
        )
        await interaction.edit_original_response(embed=progress_embed)
        
        # Fetch player's recent matches
        try:
            matches_data = await asyncio.get_event_loop().run_in_executor(
                None, fetch_valorant_matches, region.value, username, tag
            )
        except Exception as e:
            await interaction.edit_original_response(content=f"❌ Error fetching matches: {str(e)}")
            return
        
        if not matches_data:
            await interaction.edit_original_response(content="❌ No match data found for this player.")
            return
        
        matches = matches_data[:count]  # Limit to requested count
        logged_count = 0
        skipped_count = 0
        errors = []
        
        # Process each match
        for i, match in enumerate(matches, 1):
            try:
                # Update progress more frequently
                progress_embed.description = f"Processing match {i}/{len(matches)} for **{username}#{tag}**..."
                progress_embed.add_field(
                    name="📊 Progress",
                    value=f"✅ Logged: {logged_count} | ⏭️ Skipped: {skipped_count} | ❌ Errors: {len(errors)}",
                    inline=False
                )
                await interaction.edit_original_response(embed=progress_embed)
                
                # Clear the progress field for next update
                progress_embed.clear_fields()
                
                match_id = match['metadata']['matchid']
                
                # Fetch detailed match data with timeout protection
                try:
                    match_details = await asyncio.get_event_loop().run_in_executor(
                        None, fetch_match_details, match_id
                    )
                except Exception as fetch_error:
                    errors.append(f"Match {i}: Fetch error - {str(fetch_error)}")
                    continue
                
                if match_details:
                    # Try to log the match
                    try:
                        result = log_match_data(match_details, username, tag, region.value)
                        if result:  # Match was logged (not a duplicate)
                            logged_count += 1
                        else:
                            skipped_count += 1
                    except Exception as log_error:
                        errors.append(f"Match {i}: Log error - {str(log_error)}")
                else:
                    errors.append(f"Match {i}: Could not fetch detailed data - empty response")
                
                # Small delay to avoid rate limiting and give time for progress updates
                await asyncio.sleep(0.5)
                
            except Exception as match_error:
                errors.append(f"Match {i}: General error - {str(match_error)}")
        
        # Create results embed
        results_embed = discord.Embed(
            title="✅ Match Data Pull Complete",
            color=0x00FF00 if logged_count > 0 else 0xFFFF00
        )
        
        results_embed.add_field(
            name="📊 Summary",
            value=(
                f"**Player:** {username}#{tag}\n"
                f"**Region:** {region.name}\n"
                f"**Requested:** {count} games\n"
                f"**Processed:** {len(matches)} games"
            ),
            inline=False
        )
        
        results_embed.add_field(
            name="📥 Results",
            value=(
                f"**✅ Newly Logged:** {logged_count}\n"
                f"**⏭️ Already Logged:** {skipped_count}\n"
                f"**❌ Errors:** {len(errors)}"
            ),
            inline=False
        )
        
        if errors:
            error_text = "\n".join(errors[:5])  # Show first 5 errors
            if len(errors) > 5:
                error_text += f"\n... and {len(errors) - 5} more"
            
            results_embed.add_field(
                name="⚠️ Errors",
                value=f"```{error_text}```",
                inline=False
            )
        
        if logged_count > 0:
            results_embed.add_field(
                name="🎯 Next Steps",
                value=(
                    f"Use `/stats` to view player statistics\n"
                    f"Use `/serverstats` for server analytics\n"
                    f"Use `/firstblood` for first blood analysis"
                ),
                inline=False
            )
        
        results_embed.set_footer(text="💡 This command is primarily for testing and data collection")
        
        await interaction.edit_original_response(embed=results_embed)
        
    except discord.NotFound:
        # Interaction token expired
        logging.error("Pull games command: Interaction token expired")
        return
    except Exception as e:
        logging.error(f"Error in pull games command: {e}")
        try:
            await interaction.edit_original_response(content="❌ Error pulling match data.")
        except discord.NotFound:
            logging.error("Pull games command: Could not send error response - interaction expired")

# Error handling for command errors
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return  # Ignore unknown commands
    
    elif isinstance(error, commands.MissingRequiredArgument):
        error_embed = discord.Embed(
            title="❌ Missing Arguments",
            description="Please check the command usage. Use `/help` for help.",
            color=0xFF0000
        )
        await ctx.send(embed=error_embed)
    
    else:
        logging.error(f"Unhandled error: {error}")
        error_embed = discord.Embed(
            title="❌ Error",
            description="An unexpected error occurred. Please try again later.",
            color=0xFF0000
        )
        await ctx.send(embed=error_embed)

# Error handling for slash commands
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        if isinstance(error, app_commands.CommandOnCooldown):
            error_embed = discord.Embed(
                title="⏰ Command on Cooldown",
                description=f"Please wait {error.retry_after:.2f} seconds before using this command again.",
                color=0xFF0000
            )
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
        elif isinstance(error, app_commands.CommandInvokeError):
            # Check if it's a NotFound error (interaction expired)
            if isinstance(error.original, discord.NotFound):
                logging.error(f"Interaction expired for command: {interaction.command.name if interaction.command else 'unknown'}")
                return  # Can't respond to expired interaction
            
            logging.error(f"Command invoke error: {error.original}")
            error_embed = discord.Embed(
                title="❌ Command Error",
                description="An error occurred while executing the command. Please try again.",
                color=0xFF0000
            )
            if interaction.response.is_done():
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
        else:
            logging.error(f"Unhandled app command error: {error}")
            error_embed = discord.Embed(
                title="❌ Error",
                description="An unexpected error occurred. Please try again later.",
                color=0xFF0000
            )
            if interaction.response.is_done():
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
    except discord.NotFound:
        # Interaction token expired, can't respond
        logging.error("Could not send error response - interaction token expired")
    except Exception as e:
        logging.error(f"Error in error handler: {e}")

# Run the bot
if __name__ == "__main__":
    # Get bot token from environment variables
    BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
    
    if not BOT_TOKEN:
        print("❌ No Discord bot token found!")
        print("Please set DISCORD_BOT_TOKEN in your .env file")
        exit(1)
    
    try:
        print("🚀 Starting Valorant Discord Bot...")
        print(f"📊 Using Henrik-3 Valorant API v4.2.0")
        print("⚡ Using modern slash commands with dropdowns")
        
        if HENRIK_API_KEY:
            print("✅ Henrik API key loaded")
            print("📈 Rate limit: 30-90 requests/minute (depending on key type)")
        else:
            print("⚠️  No Henrik API key found")
            print("📈 Using public rate limits (limited requests)")
            print("💡 Get an API key from: https://docs.henrikdev.xyz/authentication-and-authorization")
        
        print("\n🎯 Available Commands:")
        print("• /fullmatch - Get comprehensive stats for all 10 players with agents")
        print("• /stats - View individual player statistics and trends")
        print("• /serverstats - View server-wide statistics (linked members only)")
        print("• /economy - Analyze first blood win rates and effective loadouts")
        print("• /link - Link your Discord account to your Valorant username")
        print("• /unlink - Remove your account link")
        print("• /linked - Show all linked server members")
        print("• /help - Show command help")
        print("• /api_status - Check API status")
        
        bot.run(BOT_TOKEN)
    except discord.LoginFailure:
        print("❌ Invalid bot token! Please check your token and try again.")
    except Exception as e:
        print(f"❌ Error starting bot: {e}")

# Run the bot
if __name__ == "__main__":
    # Get bot token from environment variables
    BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
    
    if not BOT_TOKEN:
        print("❌ No Discord bot token found!")
        print("Please set DISCORD_BOT_TOKEN in your .env file")
        exit(1)
    
    try:
        print("🚀 Starting Valorant Discord Bot...")
        print(f"📊 Using Henrik-3 Valorant API v4.2.0")
        print("⚡ Using modern slash commands with dropdowns")
        
        if HENRIK_API_KEY:
            print("✅ Henrik API key loaded")
            print("� Rate limit: 30-90 requests/minute (depending on key type)")
        else:
            print("⚠️  No Henrik API key found")
            print("� Using public rate limits (limited requests)")
            print("💡 Get an API key from: https://docs.henrikdev.xyz/authentication-and-authorization")
        
        print("\n🎯 Available Commands:")
        print("• /fullmatch - Get comprehensive stats for all 10 players with agents")
        print("• /stats - View individual player statistics and trends")
        print("• /serverstats - View server-wide statistics (linked members only)")
        print("• /firstblood - Analyze first blood win percentage")
        print("• /pullgames - Pull and log specific number of games for testing")
        print("• /link - Link your Discord account to your Valorant username")
        print("• /unlink - Remove your account link")
        print("• /linked - Show all linked server members")
        print("• /help - Show command help")
        print("• /api_status - Check API status")
        
        bot.run(BOT_TOKEN)
    except discord.LoginFailure:
        print("❌ Invalid bot token! Please check your token and try again.")
    except Exception as e:
        print(f"❌ Error starting bot: {e}")
