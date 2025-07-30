# 🎯 Valorant Discord Bot

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Discord.py](https://img.shields.io/badge/Discord.py-2.3.0+-blue.svg)](https://discordpy.readthedocs.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![API](https://img.shields.io/badge/API-Henrik--3%20Valorant-red.svg)](https://docs.henrikdev.xyz/)

A comprehensive Discord bot for tracking and analysing Valorant match statistics with detailed player performance metrics, server-wide analytics, and KAST calculations.

## ✨ Features

### 🎯 Match Analysis
- **Complete Match Reports** (`/fullmatch`): View detailed stats for all 10 players in a match
- **Real-time KAST Calculation**: Kill/Assist/Survive/Trade percentage with enhanced accuracy
- **Comprehensive Statistics**: ACS, ADR, HS%, K/D/A, First Kills/Deaths, Multi-kills, +/-
- **Team Separation**: Color-coded teams with proper player highlighting
- **Agent Display**: Shows which agent each player used

### 📊 Player Statistics
- **Individual Stats** (`/stats`): Personal performance tracking over customiable time periods
- **Win Rate Analysis**: Track wins/losses and performance trends
- **Agent Specialisation**: See most-played agents and best performing agents
- **Combat Metrics**: Detailed damage, headshot, and efficiency statistics

### 🌐 Server Analytics
- **Server-wide Statistics** (`/serverstats`): Comprehensive server performance analytics
- **Best Agent Per Player**: Shows each player's highest-performing agent
- **Best Individual Games**: Top ACS performances across the server
- **Map Win Rates**: Success rates on different maps (minimum 3 matches)
- **Top Performers**: Leaderboards for ACS, K/D, KAST, and damage
- **Agent Performance**: Most picked agents and best performing agents
- **First Blood Analysis**: Team win rates when securing first blood
- **Economy Insights**: Most effective loadouts and cost efficiency

### 🩸 First Blood Analysis
- **First Blood Win Rates** (`/firstblood`): Dedicated analysis of first blood tactical advantage
- **Strategic Impact**: Win rate analysis when securing first kills
- **Tactical Insights**: Data-driven first blood advantage metrics

### 📥 Testing & Data Collection
- **Pull Games** (`/pullgames`): Pull and log specific number of recent games for testing
- **Bulk Data Collection**: Fetch multiple matches for analysis and testing
- **Development Tool**: Primarily for testing and data validation

### 💰 Economy & Tactical Analysis
- **Economy Analysis** (`/economy`): Detailed loadout effectiveness and first blood impact
- **First Blood Win Rates**: Statistical advantage of securing first kills
- **Effective Loadouts**: Cost-efficient weapon combinations (excludes pistol rounds)
- **Economic Efficiency**: Win rate per dollar spent on equipment
- **Tactical Insights**: Data-driven recommendations for team economy

### 🔗 Account Management
- **Account Linking** (`/link`): Connect Discord accounts to Valorant usernames
- **Server-only Statistics**: Filter analytics to show only linked server members
- **Privacy Controls**: Easy unlinking and server-specific data

### � Utility Features
- **API Status Monitoring** (`/api_status`): Check Henrik API status and rate limits
- **KAST Debugging** (`/kastdebug`): Debug KAST calculations for specific matches
- **Comprehensive Help** (`/help`): Detailed command documentation

## 🚀 Setup

### Prerequisites
- Python 3.8 or higher
- Discord Bot Token
- Henrik API Key

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/tietoa-Bobby/valo-bot.git
   cd valo-bot
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create environment file**
   Copy `.env.example` to `.env` and fill in your tokens:
   ```env
   DISCORD_BOT_TOKEN=your_discord_bot_token_here
   HENRIK_API_KEY=your_henrik_api_key_here
   ```

4. **Run the bot**
   ```bash
   python valorant_bot.py
   ```

### Getting API Keys

#### Discord Bot Token
1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Create a new application
3. Go to "Bot" section
4. Copy the token
5. Add bot to your server with appropriate permissions

#### Henrik API Key (Optional but Recommended)
1. Join [Henrik Discord Server](https://discord.com/invite/X3GaVkX2YN)
2. Verify your account
3. Go to #get-a-key channel
4. Select 'VALORANT' from dropdown
5. Choose 'Basic' (30 req/min) or 'Advanced' (90 req/min)

## 📋 Commands

### 🎯 Match Analysis
- `/fullmatch region:<region> username:<name> tag:<tag>` - Get comprehensive match stats
- `/kastdebug match_id:<id> username:<name> tag:<tag>` - Debug KAST calculations

### 📊 Statistics & Analysis
- `/stats username:<name> tag:<tag> [days:<1-30>]` - View player statistics
- `/serverstats [days:<1-90>] [server_only:<true/false>]` - Server-wide analytics
- `/economy [days:<1-90>] [server_only:<true/false>]` - Economy and tactical analysis
- `/firstblood [days:<1-90>] [server_only:<true/false>]` - First blood win rate analysis

### 📥 Testing & Data Collection
- `/pullgames region:<region> username:<name> tag:<tag> [count:<1-20>]` - Pull and log games for testing

### 🔗 Account Management
- `/link username:<name> tag:<tag>` - Link Discord to Valorant account
- `/unlink` - Remove account link
- `/linked` - Show all linked server members

### ℹ️ Information
- `/help` - Show detailed command help
- `/api_status` - Check API status and rate limits

## 📈 Statistics Explained

### KAST Percentage
**K**ill **A**ssist **S**urvive **T**rade percentage measures round impact:
- **Kill**: Player eliminated an enemy
- **Assist**: Player assisted in eliminating an enemy
- **Survive**: Player survived the round
- **Trade**: Player was eliminated but a teammate traded the kill within 5 seconds

### ACS (Average Combat Score)
Measures overall combat effectiveness based on:
- Damage dealt
- Kills and assists
- Multi-kills
- First kills

### Other Metrics
- **ADR**: Average Damage per Round
- **HS%**: Headshot percentage
- **FK/FD**: First Kills / First Deaths per match
- **MK**: Multi-kills (3K+ eliminations in a round)
- **+/-**: Kill/Death differential
- **First Blood Win Rate**: Team win percentage when securing first kill
- **Economy Efficiency**: Win rate per credit spent on loadouts

## 🗂️ Data Storage

The bot automatically creates and manages:
- `match_logs/` - Daily match data in JSON format
- `user_links.json` - Discord to Valorant account links

Data is organised by date for efficient querying and analysis.

## 🌍 Supported Regions

- **EU** - Europe
- **NA** - North America  
- **AP** - Asia Pacific
- **KR** - Korea

## 🔒 Privacy & Data

- Match data is logged locally for statistics
- Account links are server-specific
- Users can unlink accounts anytime
- No sensitive data is stored
- Data retention is user-controlled

## 🛠️ Technical Details

### API Integration
- **Henrik-3 Valorant API**: Primary data source
- **Rate Limiting**: Automatic handling with key-based limits
- **Error Handling**: Comprehensive error management
- **Caching**: Efficient data retrieval and storage

### Bot Permissions Required
- **Send Messages** - To respond to commands
- **Use Slash Commands** - For modern Discord interactions
- **Embed Links** - To send styled match reports
- **Message Content Intent** - Required privileged intent

### Performance Features
- **Async Operations**: Non-blocking command execution
- **Smart Filtering**: Server-only statistics for privacy
- **Efficient Queries**: Optimised data processing
- **Memory Management**: Automatic cleanup and organisation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/tietoa-Bobby/valo-bot.git
cd valo-bot

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
# Edit .env with your tokens

# Run the bot
python valorant_bot.py
```

## 🚨 Troubleshooting

### Common Issues

#### Bot not responding to commands
- Ensure bot has proper permissions in your Discord server
- Check that bot is online and connected
- Verify the bot has "Send Messages" and "Use Slash Commands" permissions

#### "Player not found" errors
- Verify the username is case-sensitive and exact
- Ensure the tag is just numbers/letters (no #)
- Check that the player profile is public
- Confirm the region is correct

#### API rate limit errors
- Get a Henrik API key for higher rate limits
- Wait a few minutes between requests
- Check API status with `/api_status`

#### Missing match data
- Player must have recent matches
- Profile must be public
- Some matches may not be tracked by the API

#### Command timeout errors
- Some analysis commands (like `/economy`) may take time to process large datasets
- The bot will show progress updates during processing
- If a command times out, try using fewer days or use `/pullgames` first to ensure data is available
- Server-only filtering (`server_only:True`) can speed up processing

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Henrik-3](https://docs.henrikdev.xyz/) for the excellent Valorant API
- [discord.py](https://discordpy.readthedocs.io/) for the Discord bot framework