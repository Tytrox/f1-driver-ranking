# f1-driver-ranking

Ranks Formula 1 drivers by comparing their historical performance in the same machinery.

## Concept

Comparing the performance of two teammates in F1 is relatively straightforward,
as (for the most part) the cars they are racing should have the same performance.
Directly comparing the performance of two drivers which have never been teammates
is not as simple however, as the cars they are racing will almost always perform
differently.

Fortunately, F1 drivers typically have many teammates over their career. This 
project attempts to rank Formula 1 drivers which have never been teammates
by comparing their performance to a shared teammate, or series of teammates.

This approach does have significant flaws however, as 
[discussed below](#flaws-with-this-approach).

## Requirements

- python3
- Make
- pip3
- Apache Spark
- Internet connection

Implemented and tested on AlmaLinux.

I expect this to work on other Linux distributions, and probably MacOS.

## Usage

```bash
make current_season_sample_results.csv
```

This will sample the various driver's positions in relation to each other,
and report the mean position of each driver (explained further 
[below](#consequences)).

## Data origin

The data in the [csv_data](csv_data) directory was retrieved from the 
[Ergast Developer API](https://ergast.com/mrd/) via 
[this download link](https://ergast.com/downloads/f1db_csv.zip) on 27/04/2024.

## Comparison method

### Direct teammates

If two drivers have raced in the same team at the same time, I make the following
assumptions:

1. The cars they are driving are equivalently quick.
2. The race strategies they use are equivalent.
3. They are both attempting to finish the race as fast as possible.

These are clearly incorrect assumptions (as 
[discussed below](#flaws-with-this-approach), however I did not have the data 
(or time) to account for when these assumptions are violated.

For each race, I then only compare the laps all drivers of the same team completed.
I take the sum of each driver's race time for these laps, and subtract the teammate's
comparable race time from this sum to create a __time delta__ for that race. I then
take a weighted mean of these time deltas (weighted by the number of comparable laps)
for each race the two drivers were teammates.

### Indirect teammates

I use breadth first search to explore the relation graph of teammates, and compare all
shortest length and shortest length + 1 paths (including direct teammates - I also
compare the performance with a shared teammate).

The time delta for each path is simply the sum of time deltas for adjacent teammates on
that path.

I then take a weighted average over all paths, weighted by the minimum number of 
shared laps between teammates along the path (i.e. the bottleneck).

The reasoning behind this is the assumption that the quantity of "evidence"
for this time delta is limited by "the weakest link" in the chain of shared
teammates.

For example, comparing the performance of George Russell with Valtteri Bottas could be
done via two paths: either directly via their shared race at Sakhir 2020, or via their
shared teammate, Lewis Hamilton. My thinking is that due to the smaller number of
shared laps as direct teammates makes for a weaker comparison compared to their 
shared laps with Lewis.

## Flaws with this approach

1. Teammates are not always in equivalent machinery (e.g. Alex Albon and Logan Sargeant).
2. The race strategies are not always equivalent.
3. Drivers don't always attempt to finish as fast as possible (e.g. Sergio Perez 2021 Abu Dhabi)
4. A driver's performance may vary over time and with the car they are driving (e.g. Daniel Ricciardo was probably ill suited to the McLaren, but well suited to the Red Bull)

### Consequences

The `compare_drivers` function in [compare_drivers.py](./compare_drivers.py) is not always
consistent. For example, in an early version it said Lando was on average faster than Lewis
and slower than Carlos, however it said that Lewis was faster than Carlos.

Therefore, this function cannot provide a total ordering on the performance of drivers.

In order to attempt to attain a driver ranking from this function, I attempted a form of 
"sampling". I first randomise the list of drivers to sort, followed by using bubble sort
and the `compare_drivers` function to sort the list. I then record the position of each 
driver in that list, and repeat (100,000 times). I then take the mean of each driver's
position to create an order.

Due to the random nature of this sampling, when two drivers have very similar performance
it is incredibly unclear which should be rated higher. However, if a driver has significantly
different average performance to another, we can gain some (perhaps flawed) insight.

## Results

From [Table 1](#table-1), we can draw the following insights about the current season's F1 drivers:

1. Max Verstappen is consistently faster than all other drivers.
2. Alonso and Norris are the next fastest, with Alonso probably being slightly faster than Norris
3. Albon, Hulkenberg, Hamilton, Ricciardo, Gasly, Sainz, Leclerc and Tsunoda probably are very 
  similarly fast, and are probably slightly slower than group 2 above.
4. Bottas, Russel and Ocon are probably similarly fast, and slightly slower than group 3 above.
5. Piastri, Perez, Zhou and Magnussen are probably similarly fast, and slightly slower than group 4 above.
6. Stroll and Sargeant are similarly fast, and slightly slower than group 5 above.

### NOTE:

The reason why a certain driver is faster is not necessarily only due to driver skill - 
see [flaws with this approach](#flaws-with-this-approach).

For example, Bearman has only raced once in F1, with very little practice or experience in an F1
car. __This does not mean that he is similarly paced to Stroll and Sargeant__.

### Table 1

Created using the same command in [usage](#usage), as described by the 
[comparison method](#comparison-method) section above.

|driverRef      |meanPosition|
|---------------|------------|
|max_verstappen |0.0         |
|alonso         |3.98801     |
|norris         |4.60191     |
|albon          |6.40211     |
|hulkenberg     |6.45392     |
|hamilton       |6.64841     |
|ricciardo      |6.88564     |
|gasly          |6.8938      |
|sainz          |7.29342     |
|leclerc        |7.41055     |
|tsunoda        |7.47211     |
|bottas         |11.53661    |
|russell        |11.55489    |
|ocon           |11.67818    |
|piastri        |14.35285    |
|perez          |14.72723    |
|zhou           |15.14488    |
|kevin_magnussen|15.73321    |
|stroll         |16.55041    |
|bearman        |16.84923    |
|sargeant       |17.82263    |
