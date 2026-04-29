# Basketball Analytics - Enriched Tables Reference

## Table of Contents

**Atomic Per-Game Tables**
- [gamecompetitiveness_enriched](#gamecompetitiveness_enriched) - Game competitiveness and flow metrics
- [playergameplusminus_enriched](#playergameplusminus_enriched) - Player plus/minus per game
- [playergameimpact_enriched](#playergameimpact_enriched) - Player offensive/defensive impact per game
- [playergamequarterstats_enriched](#playergamequarterstats_enriched) - Player quarter performance per game
- [playergame_shots_enriched](#playergame_shots_enriched) - Player shot locations per game
- [playerclutchperformance_enriched](#playerclutchperformance_enriched) - Player clutch performance per game
- [teamgameanalytics_enriched](#teamgameanalytics_enriched) - Team box scores per game
- [teamgamequarterperformance_enriched](#teamgamequarterperformance_enriched) - Team quarter breakdowns per game
- [teamgame_shots_enriched](#teamgame_shots_enriched) - Team shot locations per game
- [fiveplayer_combinations_enriched](#fiveplayer_combinations_enriched) - 5-player lineup performance per game
- [threeplayer_combinations_enriched](#threeplayer_combinations_enriched) - 3-player combination performance per game

**Wide Per-Game Tables**
- [playergameanalytics_enriched](#playergameanalytics_enriched) - Comprehensive player game analytics

**Aggregated Season Tables**
- [playerstatssummary_enriched](#playerstatssummary_enriched) - Player season box score totals
- [player2ptsummary_enriched](#player2ptsummary_enriched) - Player season 2PT shot locations
- [player3ptsummary_enriched](#player3ptsummary_enriched) - Player season 3PT shot locations
- [playerplusminus_enriched](#playerplusminus_enriched) - Player career plus/minus
- [playeranalytics_enriched](#playeranalytics_enriched) - Comprehensive player season analytics
- [teamstatssummary_enriched](#teamstatssummary_enriched) - Team season box score totals
- [opponentsstatssummary_enriched](#opponentsstatssummary_enriched) - Opponent season statistics
- [opponentstrendssummary_enriched](#opponentstrendssummary_enriched) - Performance vs opponent averages
- [teamhomeawaysplits_enriched](#teamhomeawaysplits_enriched) - Team home/away season splits
- [teamquarterperformance_enriched](#teamquarterperformance_enriched) - Team season performance by quarter
- [teamanalytics_enriched](#teamanalytics_enriched) - Comprehensive team season analytics

---

## Atomic Per-Game Tables

### gamecompetitiveness_enriched
Game competitiveness classification and flow metrics per game.

| Column | Type | Description |
|--------|------|-------------|
| `game_uuid` | String | Unique game identifier |
| `season` | Integer | Season year |
| `game_date` | Date | Game date |
| `home_team_uuid` | String | Home team identifier |
| `away_team_uuid` | String | Away team identifier |
| `home_final_score` | Integer | Home team final score |
| `away_final_score` | Integer | Away team final score |
| `final_margin` | Integer | Absolute point difference at game end |
| `largest_lead` | Integer | Maximum lead by either team |
| `lead_changes` | Integer | Number of times lead changed hands |
| `game_type` | String | Classification: competitive, comfortable, or blowout |
| `competitive_until_minute` | Integer | Minute when game stopped being close |
| `comeback_win` | Boolean | Team trailed in Q4 but won |
| `winning_team_uuid` | String | Team that won the game |

---

### playergameplusminus_enriched
Player plus/minus rating for each game appearance.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `game_uuid` | String | Unique game identifier |
| `total_plus_minus` | Integer | Cumulative point differential while on court |
| `avg_plus_minus` | Float | Average plus/minus per stint |

---

### playergameimpact_enriched
Player offensive and defensive impact metrics per game.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `game_uuid` | String | Unique game identifier |
| `offensive_points_on_court` | Integer | Points scored by team while player on court |
| `defensive_points_on_court` | Integer | Points allowed while player on court |
| `offensive_points_per_minute` | Float | Offensive scoring rate while on court |
| `defensive_points_per_minute` | Float | Defensive points allowed rate |

---

### playergamequarterstats_enriched
Player quarter-specific performance metrics per game.

| Column | Type | Description |
|--------|------|-------------|
| `game_uuid` | String | Unique game identifier |
| `player_uuid` | String | Unique player identifier |
| `quarters_started` | Integer | Number of quarters player was in starting lineup |
| `quarters_won_when_starting` | Integer | Quarters won when player started |
| `quarter_win_rate_when_starting` | Float | Win rate for quarters started |

---

### playergame_shots_enriched
Player shot locations and shooting efficiency per game.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `season` | Integer | Season year |
| `game_uuid` | String | Unique game identifier |
| `game_date` | Date | Game date |
| `twopoint_locations` | List[Struct] | Array of 2PT made shot coordinates (x, y) |
| `threepoint_locations` | List[Struct] | Array of 3PT made shot coordinates (x, y) |
| `two_pt_made` | Integer | Total 2PT field goals made |
| `two_pt_attempted` | Integer | Total 2PT field goal attempts |
| `three_pt_made` | Integer | Total 3PT field goals made |
| `three_pt_attempted` | Integer | Total 3PT field goal attempts |

---

### playerclutchperformance_enriched
Player performance in high-pressure game situations.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `game_uuid` | String | Unique game identifier |
| `season` | Integer | Season year |
| `game_date` | Date | Game date |
| `is_clutch_game` | Boolean | Game margin ≤5 points in Q4 |
| `fourth_quarter_points` | Integer | Points scored in fourth quarter |
| `fourth_quarter_minutes` | Integer | Minutes played in fourth quarter |
| `clutch_points` | Integer | Points scored in clutch moments |
| `clutch_minutes` | Float | Minutes played in clutch time |
| `clutch_shooting_attempts` | Integer | Shot attempts in clutch situations |
| `clutch_shooting_made` | Integer | Made shots in clutch situations |
| `clutch_shooting_pct` | Float | Shooting percentage in clutch time |
| `game_result` | String | Win or Loss for player's team |

---

### teamgameanalytics_enriched
Comprehensive team box scores and opponent stats per game.

| Column | Type | Description |
|--------|------|-------------|
| `game_uuid` | String | Unique game identifier |
| `team_uuid` | String | Unique team identifier |
| `team_name` | String | Full team name |
| `team_short_name` | String | Team abbreviation |
| `team_uuid_opponent` | String | Opponent team identifier |
| `team_name_opponent` | String | Opponent full name |
| `team_short_name_opponent` | String | Opponent abbreviation |
| `season` | Integer | Season year |
| `team_type` | String | Home or away |
| `game_time` | Date | Game date |
| `points` | Integer | Team total points |
| `ft_attempted` | Integer | Free throw attempts |
| `ft_made` | Integer | Free throws made |
| `two_made` | Integer | 2PT field goals made |
| `three_made` | Integer | 3PT field goals made |
| `assists` | Integer | Total assists |
| `rebounds` | Integer | Total rebounds |
| `steals` | Integer | Total steals |
| `fouls` | Integer | Personal fouls committed |
| `points_opponent` | Integer | Opponent total points |
| `ft_attempted_opponent` | Integer | Opponent free throw attempts |
| `ft_made_opponent` | Integer | Opponent free throws made |
| `two_made_opponent` | Integer | Opponent 2PT made |
| `three_made_opponent` | Integer | Opponent 3PT made |
| `assists_opponent` | Integer | Opponent assists |
| `rebounds_opponent` | Integer | Opponent rebounds |
| `steals_opponent` | Integer | Opponent steals |
| `fouls_opponent` | Integer | Opponent fouls |

---

### teamgamequarterperformance_enriched
Team quarter-by-quarter performance breakdown per game.

| Column | Type | Description |
|--------|------|-------------|
| `game_uuid` | String | Unique game identifier |
| `team_uuid` | String | Unique team identifier |
| `team_name` | String | Full team name |
| `team_short_name` | String | Team abbreviation |
| `opponent_uuid` | String | Opponent team identifier |
| `opponent_name` | String | Opponent full name |
| `season` | Integer | Season year |
| `game_date` | Date | Game date |
| `team_type` | String | Home or away |
| `quarter_1_points` | Integer | Points scored in Q1 |
| `quarter_2_points` | Integer | Points scored in Q2 |
| `quarter_3_points` | Integer | Points scored in Q3 |
| `quarter_4_points` | Integer | Points scored in Q4 |
| `quarter_1_points_allowed` | Integer | Points allowed in Q1 |
| `quarter_2_points_allowed` | Integer | Points allowed in Q2 |
| `quarter_3_points_allowed` | Integer | Points allowed in Q3 |
| `quarter_4_points_allowed` | Integer | Points allowed in Q4 |
| `quarter_1_margin` | Integer | Point differential in Q1 |
| `quarter_2_margin` | Integer | Point differential in Q2 |
| `quarter_3_margin` | Integer | Point differential in Q3 |
| `quarter_4_margin` | Integer | Point differential in Q4 |
| `quarters_won` | Integer | Count of quarters won |
| `quarters_lost` | Integer | Count of quarters lost |
| `quarters_tied` | Integer | Count of quarters tied |
| `largest_lead_q1` | Integer | Maximum lead in Q1 |
| `largest_lead_q2` | Integer | Maximum lead in Q2 |
| `largest_lead_q3` | Integer | Maximum lead in Q3 |
| `largest_lead_q4` | Integer | Maximum lead in Q4 |

---

### teamgame_shots_enriched
Team shot locations and shooting efficiency per game.

| Column | Type | Description |
|--------|------|-------------|
| `team_uuid` | String | Unique team identifier |
| `team_name` | String | Full team name |
| `team_short_name` | String | Team abbreviation |
| `season` | Integer | Season year |
| `game_uuid` | String | Unique game identifier |
| `game_date` | Date | Game date |
| `opponent_uuid` | String | Opponent team identifier |
| `opponent_name` | String | Opponent full name |
| `team_type` | String | Home or away |
| `twopoint_locations` | List[Struct] | Array of 2PT made shot coordinates (x, y) |
| `threepoint_locations` | List[Struct] | Array of 3PT made shot coordinates (x, y) |
| `two_pt_made` | Integer | Total 2PT field goals made |
| `two_pt_attempted` | Integer | Total 2PT field goal attempts |
| `three_pt_made` | Integer | Total 3PT field goals made |
| `three_pt_attempted` | Integer | Total 3PT field goal attempts |
| `two_pt_pct` | Float | 2PT shooting percentage |
| `three_pt_pct` | Float | 3PT shooting percentage |

---

### fiveplayer_combinations_enriched
Performance of 5-player lineup combinations per game.

| Column | Type | Description |
|--------|------|-------------|
| `game_uuid` | String | Unique game identifier |
| `team_uuid` | String | Unique team identifier |
| `team_name` | String | Full team name |
| `season` | Integer | Season year |
| `game_date` | Date | Game date |
| `opponent` | String | Opponent team name |
| `lineup_id` | String | Unique lineup identifier (concatenated player IDs) |
| `player_1_uuid` | String | First player in lineup |
| `player_2_uuid` | String | Second player in lineup |
| `player_3_uuid` | String | Third player in lineup |
| `player_4_uuid` | String | Fourth player in lineup |
| `player_5_uuid` | String | Fifth player in lineup |
| `minutes` | Float | Minutes lineup played together |
| `plus_minus` | Integer | Point differential while lineup on court |
| `court_result` | String | Won, Lost, or Draw |
| `win_rate` | Float | Win rate (1.0 = won, 0.0 = lost, 0.5 = draw) |

---

### threeplayer_combinations_enriched
Performance of 3-player combinations per game.

| Column | Type | Description |
|--------|------|-------------|
| `game_uuid` | String | Unique game identifier |
| `team_uuid` | String | Unique team identifier |
| `team_name` | String | Full team name |
| `season` | Integer | Season year |
| `game_date` | Date | Game date |
| `opponent` | String | Opponent team name |
| `combo_id` | String | Unique combination identifier (concatenated player IDs) |
| `player_1_uuid` | String | First player in combination |
| `player_2_uuid` | String | Second player in combination |
| `player_3_uuid` | String | Third player in combination |
| `minutes` | Float | Minutes combination played together |
| `plus_minus` | Integer | Point differential while combination on court |
| `court_result` | String | Won, Lost, or Draw |
| `win_rate` | Float | Win rate (1.0 = won, 0.0 = lost, 0.5 = draw) |

---

## Wide Per-Game Tables

### playergameanalytics_enriched
Comprehensive player game analytics joining all per-game metrics.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `player_name` | String | Player full name |
| `player_number` | Integer | Jersey number |
| `team_uuid` | String | Player's team identifier |
| `team_name` | String | Player's team full name |
| `team_short_name` | String | Player's team abbreviation |
| `team_uuid_opponent` | String | Opponent team identifier |
| `team_name_opponent` | String | Opponent full name |
| `team_short_name_opponent` | String | Opponent abbreviation |
| `season` | Integer | Season year |
| `game_uuid` | String | Unique game identifier |
| `game_time` | Date | Game date |
| `points` | Integer | Total points scored |
| `ft_attempted` | Integer | Free throw attempts |
| `ft_made` | Integer | Free throws made |
| `two_made` | Integer | 2PT field goals made |
| `three_made` | Integer | 3PT field goals made |
| `minutes_played` | Integer | Total minutes played |
| `total_plus_minus` | Integer | Cumulative plus/minus |
| `avg_plus_minus` | Float | Average plus/minus per stint |
| `offensive_points_on_court` | Integer | Team points while player on court |
| `defensive_points_on_court` | Integer | Opponent points while player on court |
| `offensive_points_per_minute` | Float | Offensive rate while on court |
| `defensive_points_per_minute` | Float | Defensive rate while on court |
| `quarters_started` | Integer | Quarters player started |
| `quarters_won_when_starting` | Integer | Quarters won when starting |
| `quarter_win_rate_when_starting` | Float | Win rate for quarters started |
| `twopoint_locations` | List[Struct] | Array of 2PT made shot coordinates |
| `threepoint_locations` | List[Struct] | Array of 3PT made shot coordinates |
| `is_clutch_game` | Boolean | Game margin ≤5 points in Q4 |
| `fourth_quarter_points` | Integer | Points scored in Q4 |
| `fourth_quarter_minutes` | Float | Minutes played in Q4 |
| `clutch_points` | Integer | Points in clutch time |
| `clutch_minutes` | Float | Minutes in clutch time |
| `clutch_ft_made` | Integer | Clutch free throws made |
| `clutch_ft_attempted` | Integer | Clutch free throw attempts |
| `clutch_two_made` | Integer | Clutch 2PT made |
| `clutch_two_attempted` | Integer | Clutch 2PT attempts |
| `clutch_three_made` | Integer | Clutch 3PT made |
| `clutch_three_attempted` | Integer | Clutch 3PT attempts |
| `clutch_shooting_pct` | Float | Clutch shooting percentage |
| `game_result` | String | Win or Loss for player's team |

---

## Aggregated Season Tables

### playerstatssummary_enriched
Player season box score totals and averages.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `season` | Integer | Season year |
| `total_games` | Integer | Games played |
| `total_points` | Integer | Season cumulative points |
| `total_minutes` | Integer | Season cumulative minutes |
| `total_ft_attempted` | Integer | Season free throw attempts |
| `total_ft_made` | Integer | Season free throws made |
| `total_two_made` | Integer | Season 2PT made |
| `total_three_made` | Integer | Season 3PT made |
| `average_points` | Float | Points per game |
| `average_minutes` | Float | Minutes per game |
| `avg_two_made` | Float | 2PT made per game |
| `avg_three_made` | Float | 3PT made per game |
| `avg_ft_made` | Float | Free throws made per game |
| `avg_ft_attempted` | Float | Free throw attempts per game |
| `std_points` | Float | Standard deviation of points |
| `min_points` | Integer | Lowest scoring game |
| `max_points` | Integer | Highest scoring game |

---

### player2ptsummary_enriched
Player season 2PT shot location summary.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `season` | Integer | Season year |
| `twopoint_locations` | List[Struct] | All 2PT made shot coordinates for season |

---

### player3ptsummary_enriched
Player season 3PT shot location summary.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `season` | Integer | Season year |
| `threepoint_locations` | List[Struct] | All 3PT made shot coordinates for season |

---

### playerplusminus_enriched
Player career plus/minus totals.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `total_plus_minus` | Integer | Career cumulative plus/minus |
| `avg_plus_minus` | Float | Career average plus/minus per game |

---

### playeranalytics_enriched
Comprehensive player season analytics joining multiple season tables.

| Column | Type | Description |
|--------|------|-------------|
| `player_uuid` | String | Unique player identifier |
| `player_name` | String | Player full name |
| `player_number` | Integer | Jersey number |
| `team_uuid` | String | Player's team identifier |
| `team_name` | String | Player's team full name |
| `season` | Integer | Season year |
| `total_games` | Integer | Games played |
| `total_points` | Integer | Season cumulative points |
| `total_minutes` | Integer | Season cumulative minutes |
| `total_ft_attempted` | Integer | Season free throw attempts |
| `total_ft_made` | Integer | Season free throws made |
| `total_two_made` | Integer | Season 2PT made |
| `total_three_made` | Integer | Season 3PT made |
| `average_points` | Float | Points per game |
| `average_minutes` | Float | Minutes per game |
| `avg_two_made` | Float | 2PT made per game |
| `avg_three_made` | Float | 3PT made per game |
| `avg_ft_made` | Float | Free throws made per game |
| `avg_ft_attempted` | Float | Free throw attempts per game |
| `std_points` | Float | Standard deviation of points |
| `min_points` | Integer | Lowest scoring game |
| `max_points` | Integer | Highest scoring game |
| `total_plus_minus` | Integer | Season cumulative plus/minus |
| `avg_plus_minus` | Float | Average plus/minus per game |
| `avg_offensive_points_on_court` | Float | Average offensive points while on court |
| `avg_defensive_points_on_court` | Float | Average defensive points while on court |
| `avg_offensive_points_per_minute` | Float | Offensive scoring rate |
| `avg_defensive_points_per_minute` | Float | Defensive points allowed rate |
| `avg_quarters_started` | Float | Average quarters started per game |
| `avg_quarters_won_when_starting` | Float | Average quarters won when starting |
| `avg_quarter_win_rate` | Float | Win rate for started quarters |
| `total_clutch_games` | Integer | Number of clutch games played |
| `avg_clutch_points` | Float | Average points in clutch games |
| `avg_clutch_shooting_pct` | Float | Average clutch shooting percentage |
| `clutch_wins` | Integer | Number of clutch games won |
| `twopoint_locations` | List[Struct] | All 2PT made shot coordinates for season |
| `threepoint_locations` | List[Struct] | All 3PT made shot coordinates for season |

---

### teamstatssummary_enriched
Team season box score totals and averages.

| Column | Type | Description |
|--------|------|-------------|
| `team_uuid` | String | Unique team identifier |
| `season` | Integer | Season year |
| `total_games` | Integer | Games played |
| `total_points` | Integer | Season cumulative points |
| `total_ft_attempted` | Integer | Season free throw attempts |
| `total_ft_made` | Integer | Season free throws made |
| `total_two_made` | Integer | Season 2PT made |
| `total_three_made` | Integer | Season 3PT made |
| `average_points` | Float | Points per game |
| `avg_two_made` | Float | 2PT made per game |
| `avg_three_made` | Float | 3PT made per game |
| `avg_ft_made` | Float | Free throws made per game |
| `avg_ft_attempted` | Float | Free throw attempts per game |
| `std_points` | Float | Standard deviation of points |
| `min_points` | Integer | Lowest scoring game |
| `max_points` | Integer | Highest scoring game |

---

### opponentsstatssummary_enriched
Opponent season statistics aggregated by team.

| Column | Type | Description |
|--------|------|-------------|
| `team_uuid` | String | Team being analyzed |
| `season` | Integer | Season year |
| `average_points` | Float | Average points allowed per game |
| `avg_two_made` | Float | Average 2PT allowed per game |
| `avg_three_made` | Float | Average 3PT allowed per game |
| `avg_ft_made` | Float | Average free throws allowed per game |
| `avg_ft_attempted` | Float | Average opponent free throw attempts |
| `std_points` | Float | Standard deviation of points allowed |
| `min_points` | Integer | Fewest points allowed in a game |
| `max_points` | Integer | Most points allowed in a game |

---

### opponentstrendssummary_enriched
Team performance vs opponent season averages.

| Column | Type | Description |
|--------|------|-------------|
| `team_uuid` | String | Team being analyzed |
| `season` | Integer | Season year |
| `avg_points_diff_pct` | Float | Percentage above/below opponent's average points allowed |
| `avg_two_made_diff_pct` | Float | Percentage difference in 2PT vs opponent average |
| `avg_three_made_diff_pct` | Float | Percentage difference in 3PT vs opponent average |
| `avg_ftmade_diff_pct` | Float | Percentage difference in FT made vs opponent average |
| `avg_ftattempted_diff_pct` | Float | Percentage difference in FT attempts vs opponent average |

---

### teamhomeawaysplits_enriched
Team season performance split by home and away games.

| Column | Type | Description |
|--------|------|-------------|
| `team_uuid` | String | Unique team identifier |
| `team_name` | String | Full team name |
| `team_short_name` | String | Team abbreviation |
| `season` | Integer | Season year |
| `home_games` | Integer | Number of home games |
| `away_games` | Integer | Number of away games |
| `home_avg_points` | Float | Average points in home games |
| `away_avg_points` | Float | Average points in away games |
| `home_avg_points_allowed` | Float | Average points allowed at home |
| `away_avg_points_allowed` | Float | Average points allowed away |
| `home_avg_margin` | Float | Average point margin at home |
| `away_avg_margin` | Float | Average point margin away |
| `home_win_rate` | Float | Win rate in home games |
| `away_win_rate` | Float | Win rate in away games |
| `home_away_point_diff` | Float | Scoring difference home vs away |
| `home_court_advantage` | Float | Margin difference home vs away |
| `typical_home_game_day` | String | Most common day for home games |
| `typical_home_game_hour` | Integer | Most common hour for home games |

---

### teamquarterperformance_enriched
Team season performance aggregated by quarter.

| Column | Type | Description |
|--------|------|-------------|
| `team_uuid` | String | Unique team identifier |
| `team_name` | String | Full team name |
| `team_short_name` | String | Team abbreviation |
| `season` | Integer | Season year |
| `quarter` | Integer | Quarter number (1-4) |
| `total_games` | Integer | Games included in aggregation |
| `avg_points` | Float | Average points per quarter |
| `avg_points_allowed` | Float | Average points allowed per quarter |
| `avg_point_margin` | Float | Average point differential per quarter |
| `quarter_win_rate` | Float | Win rate for this quarter |
| `max_scoring_run` | Integer | Best scoring run in quarter |
| `avg_largest_run` | Float | Average largest run per quarter |
| `comeback_wins` | Integer | Quarters won after being behind |
| `lead_blown` | Integer | Quarters lost after leading |

---

### teamanalytics_enriched
Comprehensive team season analytics joining multiple season tables.

| Column | Type | Description |
|--------|------|-------------|
| `team_uuid` | String | Unique team identifier |
| `team_name` | String | Full team name |
| `team_short_name` | String | Team abbreviation |
| `season` | Integer | Season year |
| `total_games` | Integer | Games played |
| `total_points` | Integer | Season cumulative points |
| `total_ft_attempted` | Integer | Season free throw attempts |
| `total_ft_made` | Integer | Season free throws made |
| `total_two_made` | Integer | Season 2PT made |
| `total_three_made` | Integer | Season 3PT made |
| `average_points` | Float | Points per game |
| `avg_two_made` | Float | 2PT made per game |
| `avg_three_made` | Float | 3PT made per game |
| `avg_ft_made` | Float | Free throws made per game |
| `avg_ft_attempted` | Float | Free throw attempts per game |
| `std_points` | Float | Standard deviation of points |
| `min_points` | Integer | Lowest scoring game |
| `max_points` | Integer | Highest scoring game |
| `average_points_opponent` | Float | Average points allowed |
| `avg_two_made_opponent` | Float | Average 2PT allowed |
| `avg_three_made_opponent` | Float | Average 3PT allowed |
| `avg_ft_made_opponent` | Float | Average free throws allowed |
| `avg_ft_attempted_opponent` | Float | Average opponent free throw attempts |
| `std_points_opponent` | Float | Standard deviation points allowed |
| `min_points_opponent` | Integer | Fewest points allowed |
| `max_points_opponent` | Integer | Most points allowed |
| `avg_points_diff_pct` | Float | Performance vs opponent averages |
| `avg_two_made_diff_pct` | Float | 2PT differential vs opponent averages |
| `avg_three_made_diff_pct` | Float | 3PT differential vs opponent averages |
| `avg_ftmade_diff_pct` | Float | FT made differential vs opponent averages |
| `avg_ftattempted_diff_pct` | Float | FT attempts differential vs opponent averages |
| `home_games` | Integer | Number of home games |
| `away_games` | Integer | Number of away games |
| `home_avg_points` | Float | Average points scored at home |
| `away_avg_points` | Float | Average points scored away |
| `home_avg_points_allowed` | Float | Average points allowed at home |
| `away_avg_points_allowed` | Float | Average points allowed away |
| `home_avg_margin` | Float | Average point differential at home |
| `away_avg_margin` | Float | Average point differential away |
| `home_win_rate` | Float | Win rate at home |
| `away_win_rate` | Float | Win rate away |
| `home_away_point_diff` | Float | Point differential home vs away |
| `home_court_advantage` | Float | Margin improvement at home |
| `typical_home_game_day` | String | Most common home game day |
| `typical_home_game_hour` | Integer | Most common home game hour |
| `twopoint_locations` | List[Struct] | All 2PT made shot coordinates for season |
| `threepoint_locations` | List[Struct] | All 3PT made shot coordinates for season |

---

## Data Types Reference

| Type | Description | Example Values |
|------|-------------|----------------|
| String | Text identifier or name | "player_123", "FC Barcelona" |
| Integer | Whole number | 85, 12, -3 |
| Float | Decimal number | 23.5, 0.456, -1.2 |
| Date | Calendar date | 2025-11-15 |
| Boolean | True/False value | true, false |
| List[Struct] | Array of coordinate pairs | [{x: 7.5, y: 45.2}, {x: 8.1, y: 50.3}] |

---

## Common Query Patterns

### Find top scorers in a season
```sql
SELECT player_name, average_points
FROM playeranalytics_enriched
WHERE season = 2025
ORDER BY average_points DESC
LIMIT 10
```

### Get team home/away performance
```sql
SELECT team_name, home_win_rate, away_win_rate, home_court_advantage
FROM teamhomeawaysplits_enriched
WHERE season = 2025
ORDER BY home_court_advantage DESC
```

### Find clutch performers in close games
```sql
SELECT player_uuid, AVG(clutch_shooting_pct) as avg_clutch_pct
FROM playerclutchperformance_enriched
WHERE is_clutch_game = true AND clutch_shooting_attempts >= 5
GROUP BY player_uuid
ORDER BY avg_clutch_pct DESC
```

### Analyze team quarter trends
```sql
SELECT team_name, quarter, avg_points, quarter_win_rate
FROM teamquarterperformance_enriched
WHERE season = 2025
ORDER BY team_name, quarter
```

### Get best 5-player lineups
```sql
SELECT team_name, lineup_id, SUM(minutes) as total_minutes, AVG(plus_minus) as avg_plus_minus
FROM fiveplayer_combinations_enriched
WHERE season = 2025
GROUP BY team_name, lineup_id
HAVING total_minutes >= 20
ORDER BY avg_plus_minus DESC
LIMIT 10
```
