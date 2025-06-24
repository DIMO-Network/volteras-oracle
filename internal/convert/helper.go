package convert

const kilometersPerMile = 1.609344

func milesToKilometers(miles float64) float64 {
	return kilometersPerMile * miles
}
