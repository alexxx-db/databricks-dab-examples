import axios from "axios";
import type { TaxiTrip } from "./App";

const apiClient = axios.create({ baseURL: "/api", timeout: 10000 });

export const getTaxiTrips = async (): Promise<TaxiTrip[]> => {
  const response = await apiClient.get<TaxiTrip[]>("/taxi-trips");
  return response.data;
};
