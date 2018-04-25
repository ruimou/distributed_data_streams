/*

Package movement holds functions needed for test apps to fake their movement
along nodes of some sort. The backing data structure is essentially a graph
with arbitrary connectivity.

Public types:
	Point
	Neighbours

Public functions:
	CreateLocationGraph
	Travel

*/
package movement

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"time"
)

// Represents a co-ordinate
type Point struct {
	Y float64
	X float64
}

// Type LocationNode has a specific location, as well as a list of other
// LocationNodes that it can reach.
type Neighbours []Point

type slopeType int

const (
	POSRIGHT slopeType = iota
	NEGRIGHT
	POSLEFT
	NEGLEFT
	ZERORIGHT
	ZEROLEFT
	INFUP
	INFDOWN
)

// readln returns a single line (without the ending \n)
// from the input buffered reader.
// An error is returned if there is an error with the
// buffered reader.
func readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

/*
Function CreateLocationGraph takes a filename that it expects to be in the
required format. Using this file it will create a graph of LocationNodes
and return it, as a map with Point keys and with values as the list of
neighbours.

The file should be a list of co-ordinate pairs separated by a space. There
should be one co-ordinate pair per line.

A co-ordinate is formed by two floating point numbers. An example file
looks like the following:
3.5,1.3 9.0,-1.3
-0.3,4.5 -8.7,-4.2

Note that each number must have a decimal point, and at least one number
numeric character on each side of the decimal point. There must be no
duplicate pairs.
*/
func CreateLocationGraph(fname string) (map[Point]*Neighbours, error, Point) {
	graph := make(map[Point]*Neighbours, 0)
	firstPoint := Point{0, 0}

	f, err := os.OpenFile(fname, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err, firstPoint
	}

	// iterate over all of the coordinate pairs in the file
	r := bufio.NewReader(f)

	// Variable used for grabbing the very first point in the file
	gotFirstPoint := false

	for {
		str, err := readln(r)
		if err != nil {
			break
		}

		re := regexp.MustCompile(`(-?\d+\.\d+),(-?\d+\.\d+) (-?\d+\.\d+),(-?\d+\.\d+)`)
		match := re.FindAllString(str, -1)
		if match == nil {
			fmt.Println("Regex match failed %s", str)
			continue
		}

		re = regexp.MustCompile(`(-?\d+\.\d+)`)
		match = re.FindAllString(str, -1)

		// Parse the numbers for the first coordinate
		float1, err := strconv.ParseFloat(match[0], 64)
		if err != nil {
			fmt.Println("Parse float failed:", match[0])
			continue
		}
		float2, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			fmt.Println("Parse float failed:", match[1])
			continue
		}
		// fmt.Println("FLOAT IS: %s", strconv.FormatFloat(float1, 'f', -1, 64))
		p1 := Point{float1, float2}
		if !gotFirstPoint {
			firstPoint = p1
			gotFirstPoint = true
		}

		// Parse the numbers for the second coordinate
		float1, err = strconv.ParseFloat(match[2], 64)
		if err != nil {
			fmt.Println("Parse float failed:", match[2])
			continue
		}
		float2, err = strconv.ParseFloat(match[3], 64)
		if err != nil {
			fmt.Println("Parse float failed:", match[3])
			continue
		}
		p2 := Point{float1, float2}

		// Enter graph data for the first coordinate
		if neighbours, exists := graph[p1]; exists {
			*neighbours = append(*neighbours, p2)
		} else {
			neighbours := make(Neighbours, 1)
			neighbours[0] = p2
			graph[p1] = &neighbours
		}

		// Enter graph data for the second coordinate
		if neighbours, exists := graph[p2]; exists {
			*neighbours = append(*neighbours, p1)
		} else {
			neighbours := make(Neighbours, 1)
			neighbours[0] = p1
			graph[p2] = &neighbours
		}
	}

	return graph, nil, firstPoint
}

/*
Function Travel will loop indefinitely and continue traveling along the graph
in increments of distance 1.0.

TODO:
- configurable distance increments instead of hard-coded 1.0
- configurable update interval instead of hard-coded 100ms
*/
func Travel(start Point, graph map[Point]*Neighbours, speed float64, fn func(p Point)) {
	prevPoint := start

	neighbours, exists := graph[start]
	if !exists {
		fmt.Println("start point is not a point in the graph:", start)
		return
	}

	nextPoint := getRandomNeighbour(neighbours, prevPoint)

	// Keep running the travel function for 90s
loop:
	for timeout := time.After(30 * time.Second); ; {
		select {
		case <-timeout:
			break loop
		default:
			// get the path iterator
			pointgen := linePointsGen(prevPoint, nextPoint, speed)
			x, y, e := pointgen()

			// travel the path
			for e == nil {
				fn(Point{x, y})
				x, y, e = pointgen()
				time.Sleep(100 * time.Millisecond)
			}

			// get the next path
			neighbours, exists := graph[nextPoint]
			tmp := getRandomNeighbour(neighbours, prevPoint)
			prevPoint = nextPoint
			nextPoint = tmp
			if !exists {
				fmt.Println("Point not in map", prevPoint)
				return
			}

			fmt.Println("Prev:", prevPoint, "Next:", nextPoint)
		}
	}
}

/*
Gets a random neighbour from a list of neighbours. Will not select
prevPoint if it is the list of neighbours.
*/
func getRandomNeighbour(neighbours *Neighbours, prevPoint Point) Point {
	switch len(*neighbours) {
	case 0:
		return Point{0, 0}
	case 1:
		return (*neighbours)[0]
	case 2:
		if (*neighbours)[1] == prevPoint {
			return (*neighbours)[0]
		} else {
			return (*neighbours)[1]
		}
	default:
		idx := rand.Int() % len(*neighbours)
		for (*neighbours)[idx] == prevPoint {
			idx = rand.Int() % len(*neighbours)
		}

		return (*neighbours)[idx]
	}
}

// Get the slope and y-intercept of a line formed by two points
func getSlopeIntercept(p1 Point, p2 Point) (slope float64, intercept float64) {
	slope = (float64(p2.Y) - float64(p1.Y)) / (float64(p2.X) - float64(p1.X))
	intercept = float64(p1.Y) - slope*float64(p1.X)

	return slope, intercept
}

// Get slope type, slope, and intercept for a a pair of points
func getLineParams(p1, p2 Point) (sT slopeType, slope, intercept float64) {
	if p1.X == p2.X {
		// Check for infinite slope.
		if p2.Y > p1.Y {
			sT = INFUP
		} else {
			sT = INFDOWN
		}

		slope, intercept = 0, 0
	} else if p1.Y == p2.Y {
		// check for zero slope
		if p2.X > p1.X {
			sT = ZERORIGHT
		} else {
			sT = ZEROLEFT
		}

		slope, intercept = 0, p1.Y
	} else {
		// 4 classifications of non infinite slope based
		// on the relative positions of p1 and p2
		slope, intercept = getSlopeIntercept(p1, p2)
		if p1.X < p2.X {
			if slope > 0 {
				sT = POSRIGHT
			} else {
				sT = NEGRIGHT
			}
		} else {
			if slope > 0 {
				sT = POSLEFT
			} else {
				sT = NEGLEFT
			}
		}
	}

	return sT, slope, intercept
}

// Generates an iterator for movement along a line
func linePointsGen(p1, p2 Point, speed float64) (gen func() (x, y float64, e error)) {
	// Set up math
	slopeT, slope, _ := getLineParams(p1, p2)

	x := p1.X
	xPrev := x
	y := p1.Y
	yPrev := y
	e := fmt.Errorf("End of path reached")
	theta := math.Atan(slope)

	// Every slope type has a different iterator, since they change the
	// x and y values in different combinations, as well as do different
	// comparisons on the values.
	switch slopeT {
	case ZERORIGHT:
		return func() (float64, float64, error) {
			if x > p2.X {
				return 0, 0, e
			}

			xPrev = x
			x += speed

			return xPrev, y, nil
		}
	case ZEROLEFT:
		return func() (float64, float64, error) {
			if x < p2.X {
				return 0, 0, e
			}

			xPrev = x
			x -= speed

			return xPrev, y, nil
		}
	case POSRIGHT:
		return func() (float64, float64, error) {
			if y > p2.Y || x > p2.X {
				return 0, 0, e
			}

			yPrev = y
			xPrev = x

			y += speed * math.Sin(theta)
			x += speed * math.Cos(theta)

			return xPrev, yPrev, nil
		}
	case NEGRIGHT:
		return func() (float64, float64, error) {
			if y < p2.Y || x > p2.X {
				return 0, 0, e
			}

			yPrev = y
			xPrev = x

			y += speed * math.Sin(theta)
			x += speed * math.Cos(theta)

			return xPrev, yPrev, nil
		}
	case POSLEFT:
		return func() (float64, float64, error) {
			if y < p2.Y || x < p2.X {
				return 0, 0, e
			}

			yPrev = y
			xPrev = x

			y -= speed * math.Sin(theta)
			x -= speed * math.Cos(theta)

			return xPrev, yPrev, nil
		}
	case NEGLEFT:
		return func() (float64, float64, error) {
			if y > p2.Y || x < p2.X {
				return 0, 0, e
			}

			yPrev = y
			xPrev = x

			y -= speed * math.Sin(theta)
			x -= speed * math.Cos(theta)

			return xPrev, yPrev, nil
		}
	case INFUP:
		return func() (float64, float64, error) {
			if y > p2.Y {
				return 0, 0, e
			}

			yPrev := y
			y += speed

			return x, yPrev, nil
		}
	case INFDOWN:
		return func() (float64, float64, error) {
			if y < p2.Y {
				return 0, 0, e
			}

			yPrev := y
			y -= speed

			return x, yPrev, nil
		}
	}

	return nil
}
