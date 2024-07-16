<h1 align="center"> Spatial Interpolation </h1>

<p align="justify"> In this repository, the inverse distance weighting algorithm is used concurrently with the Spark Framework. <br>
There are 3 branches in this repository: Spark RDD, Spark Dataframe and Spark SQL.</p>

## The algorithm

<p align="justify">The Inverse Distance Weighting (IDW) algorithm is a type of interpolation method used to estimate unknown values based on known values at surrounding points. The key idea is that points closer to the location of interest have a greater influence on the estimated value than points further away. The influence of each known point is inversely proportional to its distance from the location of interest, typically raised to a power (often 2, but it can vary). This method is commonly used in geographic information systems (GIS) for spatial interpolation.</p>

<p align="center"><img src="ql_f0a999afcd9cc442cdeda04af2e8f3ec_l3.png" /></p>

Where:
- \( Z(x) \) is the estimated value (in this case, temperature) at the location \( x \).
- \( N \) is the number of known points.
- \( Z(x_i) \) is the value (in this case, temperature) at the known point \( x_i \).
- \( d(x, x_i) \) is the distance between the location \( x \) and the known point \( x_i \).
- \( p \) is the power parameter that controls the rate of distance decay (commonly set to 2, but in this repository is set to 3).

## Application

<p align="justify">Let's pretend the following situation: you know the temperature of N points, but you don't know the temperature of a specific point. Based on the temperature of the N points that you already know, you can predict the temperature of the mentioned specific point.</p>