package com.gkgd.bigscreen.entity.dwd

case class VehicleStatusChangeInfo(
                                      vehicle_id: Long,
                                      dept_id: Int,
                                      terminal_id: String,
                                      lat: Double,
                                      lng: Double,
                                      speed: Double,
                                      direction: Int,
                                      change_status: String,
                                      time: String,
                                      acc_state: Int,
                                      gprs_state: Int,
                                      online_time: Int,
                                      department_id: Int,
                                      updatetime: String,
                                      month_id: String,
                                      day_id: String
                                  )
