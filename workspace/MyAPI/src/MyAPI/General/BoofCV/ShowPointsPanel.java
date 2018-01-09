/*
 * Copyright (c) 2011-2012, Peter Abeles. All Rights Reserved.
 *
 * This file is part of BoofCV (http://boofcv.org).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package MyAPI.General.BoofCV;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.Obj.Statistics;
import MyCustomedHaoop.ValueClass.SURFpoint;
import MyCustomedHaoop.ValueClass.SURFpoint_onlyLoc;

import java.awt.*;
import java.awt.event.MouseListener;
import java.util.List;


/**
 * Shows which two features are associated with each other.  An individual feature
 * can be shown alone by clicking on it.
 *
 */
@SuppressWarnings("serial")
public class ShowPointsPanel extends CompareTwoImagePanel_DIY implements MouseListener {
	
	public static class SURFpoint_Weight{
		public SURFpoint point;
		public float weight;
		
		public SURFpoint_Weight(SURFpoint point, float weight){//if weight<0, then this point is not displayed
			this.point=point;
			this.weight=weight;
		}
	}
	
	public static class PointLink{
		SURFpoint_onlyLoc src;
		SURFpoint_onlyLoc dsc;
		float score;
		public PointLink(SURFpoint_onlyLoc src, SURFpoint_onlyLoc dsc, float score){
			this.src=src;
			this.dsc=dsc;
			this.score=score;
		}
	}
	
	
	protected boolean isDrawOritation;
	protected List<SURFpoint_Weight> selPoints_l;
	protected List<PointLink> pointsLink_l;
	protected List<SURFpoint_Weight> selPoints_r;
	protected List<PointLink> pointsLink_r;
	protected int RGBInd;
	protected double[] scalingInfo_pointWeight;
	protected double[] scalingInfo_pointLink;
	
	public ShowPointsPanel(int borderSize, int pointEnlargeFactor, boolean isDrawOritation) {
		super(borderSize, true, pointEnlargeFactor);	
		this.isDrawOritation=isDrawOritation;
	}

	public synchronized void setPoints(List<SURFpoint_Weight> selPoints_l, List<PointLink> pointsLink_l, 
			List<SURFpoint_Weight> selPoints_r, List<PointLink> pointsLink_r, 
			int RGBInd, double[] scalingInfo_pointWeight, double[] scalingInfo_pointLink) throws InterruptedException {
		this.selPoints_l=selPoints_l;
		this.pointsLink_l=pointsLink_l;
		this.selPoints_r=selPoints_r;
		this.pointsLink_r=pointsLink_r;
		this.RGBInd=RGBInd;
		//scalingInfo_pointWeight
		if (scalingInfo_pointWeight!=null) {
			this.scalingInfo_pointWeight=scalingInfo_pointWeight;
		}else {//find min max among selPointInd_l and selPointInd_r
			if (selPoints_l==null && selPoints_r==null) {
				this.scalingInfo_pointWeight=null;
			}else{
				Statistics<String> minMax=new Statistics<>(1);
				if (selPoints_l!=null) {
					for (SURFpoint_Weight oneP : selPoints_l) {
						if (oneP.weight>=0) {
							minMax.addSample(oneP.weight, ""+oneP.point);
						}
					}
				}
				if (selPoints_r!=null) {
					for (SURFpoint_Weight oneP : selPoints_r) {
						if (oneP.weight>=0) {
							minMax.addSample(oneP.weight, ""+oneP.point);
						}
					}
				}
				this.scalingInfo_pointWeight=new double[]{minMax.getMinValue(), minMax.getMaxValue()};
			}
		}
		//scalingInfo_pointLink
		if (scalingInfo_pointLink!=null) {
			this.scalingInfo_pointLink=scalingInfo_pointLink;
		}else {//find min max among selPointInd_l and selPointInd_r
			if (pointsLink_l==null && pointsLink_r==null) {
				this.scalingInfo_pointLink=null;
			}else{
				Statistics<String> minMax=new Statistics<>(1);
				if (pointsLink_l!=null) {
					for (PointLink oneP : pointsLink_l) {
						minMax.addSample(oneP.score, ""+oneP);
					}
				}
				if (pointsLink_r!=null) {
					for (PointLink oneP : pointsLink_r) {
						minMax.addSample(oneP.score, ""+oneP);
					}
				}
				this.scalingInfo_pointLink=new double[]{minMax.getMinValue(), minMax.getMaxValue()};
			}
		}
	}

	@Override
	protected void drawFeatures(Graphics2D g2 ,
							 double scaleLeft, int leftX, int leftY,
							 double scaleRight, int rightX, int rightY) {
		//show left
		if (selPoints_l!=null) {
			for (SURFpoint_Weight oneP : selPoints_l) {
				//if weight<0, then this point is not displayed
				if (oneP.weight>=0) {
					Color color=General.getStrengthColor(RGBInd, oneP.weight, scalingInfo_pointWeight);//normalize match strength to 0~1.0
					General_BoofCV.drawInterestPoint(g2, oneP.point, scaleLeft, 0, 0, color, pointEnlargeFactor, isDrawOritation);
				}
			}
			if (pointsLink_l!=null) {
				for (PointLink oneLink : pointsLink_l) {
					Color LinkColor=General.getStrengthColor(RGBInd, oneLink.score, scalingInfo_pointLink);//normalize match strength to 0~1.0
					General_BoofCV.drawPointLink(g2, oneLink.src, oneLink.dsc, scaleLeft, 0, 0, LinkColor);
				}
			}
		}
		//show right
		if (selPoints_r!=null) {
			for (SURFpoint_Weight oneP : selPoints_r) {
				//if weight<0, then this point is not displayed
				if (oneP.weight>=0) {
					Color color=General.getStrengthColor(RGBInd, oneP.weight, scalingInfo_pointWeight);//normalize match strength to 0~1.0
					General_BoofCV.drawInterestPoint(g2, oneP.point, scaleRight, rightX, 0, color, pointEnlargeFactor, isDrawOritation);
				}
			}
			if (pointsLink_r!=null) {
				for (PointLink oneLink : pointsLink_r) {
					Color LinkColor=General.getStrengthColor(RGBInd, oneLink.score, scalingInfo_pointLink);//normalize match strength to 0~1.0
					General_BoofCV.drawPointLink(g2, oneLink.src, oneLink.dsc, scaleRight, rightX, 0, LinkColor);
				}
			}
		}
	}

	@Override
	protected boolean isValidPoint(int index) {
		return true;
	}
}
