import matplotlib
import matplotlib.pyplot as plt
from math import pi, cos, sin
import pandas as pd

#matplotlib.use('TkAgg')

def set_ring_coordinates(n_seats, distance, corridors, angle_offset=0):

    n_corridors = len(corridors)
    angle = (pi - 2*angle_offset) / (1+ n_seats + n_corridors)

    seat_coordinates = []
    for seat_number in range(1, n_seats + n_corridors + 1):
        if seat_number in corridors:
            continue
        x = distance*cos(seat_number*angle + angle_offset)
        y = distance*sin(seat_number*angle + angle_offset)
        seat_coordinates.append((x, y))
    return seat_coordinates

base_p_o = pi/20
r1_seats = set_ring_coordinates(24, 3.8, [6, 14, 22], angle_offset=base_p_o)
r2_seats = set_ring_coordinates(30, 4.6, [7, 17, 27], angle_offset=base_p_o)
r3_seats = set_ring_coordinates(36, 5.4, [7, 20, 33], angle_offset=base_p_o)
r4_seats = set_ring_coordinates(38, 6.4, [5, 9, 14, 19, 24, 29, 34, 39, 43], angle_offset=base_p_o)
r5_seats = set_ring_coordinates(46, 7.2, [5, 10, 16, 22, 28, 34, 40, 46, 51], angle_offset=base_p_o)
r6_seats = set_ring_coordinates(56, 8.0, [6, 12, 19, 26, 33, 40, 47, 54, 60], angle_offset=base_p_o)

seats = r1_seats + r2_seats + r3_seats + r4_seats + r5_seats + r6_seats


seats_df = pd.read_csv('parliament_seats_data.csv')
colors = []
for index, item in seats_df.iterrows():
    if item.loc['party'] == 'PS':
        colors.append('#F74B71')
    elif item['party'] == 'PSD':
        colors.append('#F28C26')
    elif item['party'] == 'CH':
        colors.append('#202056')
    elif item['party'] == 'IL':
        colors.append('#00AFF1')
    elif item['party'] == 'PCP':
        colors.append('#DB241C')
    elif item['party'] == 'BE':
        colors.append('#000000')
    elif item['party'] == 'PAN':
        colors.append('#0A6580')
    elif item['party'] == 'L':
        colors.append('#77BD4A')
    else:
        colors.append('ERROR ASSINING COLOR')

seats_df['color'] = pd.Series(colors)
seats_df



fig, ax = plt.subplots()
fig.set_size_inches(6,6)
ax.set_xlim([-8, 8])
ax.set_ylim([0,16])
plt.axis('off')

previous_party = seats_df['party'][0]
previous_party_color = seats_df['color'][0]
last_plotted_index = 0
last_index = len(seats_df) - 1

for index, row in seats_df.iterrows():
    # run in the last iteration of the cycle
    if index == last_index:
        ax.scatter(*zip(*seats[last_plotted_index:]), c=previous_party_color)
    elif row['party'] == previous_party:
        continue
    else:
        ax.scatter(*zip(*seats[last_plotted_index:index]), c=previous_party_color)
        previous_party = row['party']
        previous_party_color = row['color']
        last_plotted_index = index

seats_df['seat_x'] = [x for x, y in seats]
seats_df['seat_y'] = [y for x, y in seats]
seats_df.to_csv('parliament_seats_data.csv')
plt.show()
