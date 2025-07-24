
import React, { useEffect, useState } from "react";
import { Grid, Card, CardContent, Typography, CircularProgress } from "@mui/material";

const DashboardPage = () => {
  const [kpi, setKpi] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchKpi = async () => {
      try {
        const response = await fetch("/api/occupancy-dashboard");
        const data = await response.json();
        setKpi(data);
      } catch (err) {
        console.error("Error fetching KPI data:", err);
      } finally {
        setLoading(false);
      }
    };

    fetchKpi();
  }, []);

  const renderCard = (title: string, value: any, suffix: string = "") => (
    <Grid item xs={12} sm={6} md={4}>
      <Card sx={{ backgroundColor: "#1E1E1E", color: "#fff" }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>{title}</Typography>
          <Typography variant="h4">{value !== undefined ? `${value}${suffix}` : "—"}</Typography>
        </CardContent>
      </Card>
    </Grid>
  );

  if (loading) {
    return <CircularProgress color="inherit" />;
  }

  return (
    <Grid container spacing={2}>
      {renderCard("Occupancy Rate", (kpi.occupancyRate * 100).toFixed(1), "%")}
      {renderCard("Total Units", kpi.totalUnits)}
      {renderCard("Vacant Units", kpi.vacantUnits)}
      {renderCard("Total Properties", kpi.totalProperties)}
      {renderCard("Avg Rent", `$${kpi.avgRent?.toFixed(2)}`)}
      {renderCard("Leased Units", kpi.leasedUnits)}
    </Grid>
  );
};

export default DashboardPage;
